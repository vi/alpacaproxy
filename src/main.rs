use std::convert::TryInto;
use std::{net::SocketAddr, path::PathBuf};

use std::time::Duration;
use anyhow::Context;
use futures::stream::SplitSink;
use futures::{sink::SinkExt,stream::StreamExt};

use tokio_tungstenite::tungstenite::Message as WebsocketMessage;

/// Connect to Alpaca and act as a special caching proxy server for it
#[derive(argh::FromArgs)]
struct Opts {
    #[argh(positional)]
    database: PathBuf,

    #[argh(positional)]
    client_config: PathBuf,

    #[argh(positional)]
    listen_addr: SocketAddr,
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
struct Message {
    stream: String,
    data: serde_json::Value,
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
struct MinutelyData {
    ev: String,

    #[serde(rename="T")]
    t: String,

    #[serde(flatten)]
    rest: serde_json::Value,
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
struct ClientConfig {
    #[serde(with = "http_serde::uri")]
    uri: http::Uri,

    startup_messages: Vec<serde_json::Value>,
}

fn get_next_db_key(db: &sled::Db) -> anyhow::Result<u64> {
    let nextkey = match db.last()? {
        None => 0,
        Some((k, _v)) => {
            let k : [u8; 8] = k.as_ref().try_into()?;
            u64::from_be_bytes(k).checked_add(1).context("Key space is full")?
        }
    };
    Ok(nextkey)
}

async fn serve_client(mut client_socket: tokio::net::TcpStream, db: &sled::Db) -> anyhow::Result<()> {
    let (mut client_sink, mut client_stream) = tokio_tungstenite::accept_async(client_socket).await?.split();
    {
        let mut buf = serde_json::to_string(&Message{stream:"hello".to_owned(), data: serde_json::Value::Null}).unwrap();
        client_sink.send(WebsocketMessage::Text(buf)).await?;
    }
    let mut cursor = get_next_db_key(db)?;
 
    let err_to_client = |mut client_sink: SplitSink<_,_>, x:String| {
        log::warn!("Sending error to client: {}", x);
        async move {
            client_sink.send(WebsocketMessage::Text(serde_json::to_string(&Message{stream:"error".to_owned(), data:serde_json::Value::String(x)}).unwrap())).await?;
            let ret : anyhow::Result<_> = Ok(client_sink);
            ret
        }  
    };

    let handle_msg = |msg: Message, mut client_sink:  SplitSink<_,_>| -> _ {
        async move {
            match &msg.stream[..] {
                "preroll" => {
                    let num = match msg.data {
                        serde_json::Value::Number(x) => x.as_u64().context("Cannot handle nubmer of preroll messages")?,
                        _ => {
                            client_sink = err_to_client(client_sink, "Invalid type of `data` for `preroll` message, expected a number".to_owned()).await?;
                            return Ok(client_sink)
                        }
                    };
                    log::info!("  prerolling {} messages for the client", num);
                    let start = cursor.saturating_sub(num);
                    for x in start..cursor {
                        match db.get(x.to_be_bytes())? {
                            Some(v) => {
                                let minutely : MinutelyData = match serde_json::from_slice(&v) {
                                    Ok(x) => x,
                                    Err(_e) => {
                                        log::warn!("Failed to properly deserialize minutely datum number {}", x);
                                        continue
                                    }
                                };
                                let stream = format!("{}.{}", minutely.ev, minutely.t);
                                let msg = Message {
                                    stream,
                                    data: serde_json::to_value(minutely)?,
                                };
                                client_sink.send(WebsocketMessage::Text(serde_json::to_string(&msg)?)).await?;
                            }
                            None => log::warn!("Missing datum number {}", x),
                        }
                    }
                    log::info!("  prerolling finished");
                }
                x => {
                    client_sink = err_to_client(client_sink, format!("Unknown command type {}", x)).await?;
                }
            }
            let ret : anyhow::Result<_> = Ok(client_sink);
            ret
        }
    };

    while let Some(msg) = client_stream.next().await  {
        match msg {
            Ok(WebsocketMessage::Text(msg)) => client_sink = handle_msg(serde_json::from_str(&msg)?, client_sink).await?,
            Ok(WebsocketMessage::Binary(msg)) => client_sink = handle_msg(serde_json::from_slice(&msg)?, client_sink).await?,
            Ok(WebsocketMessage::Close(c)) => {
                log::warn!("Close message from upstream: {:?}", c);
            }
            Ok(WebsocketMessage::Ping(_)) => {
                log::debug!("WebSocket ping from upstream");
            }
            Ok(_) => {
                log::warn!("other WebSocket message from upstream");
            }
            Err(e) => log::error!("From upstream websocket: {}", e),
        }
    }
    Ok(())
}

async fn handle_upstream(config: &ClientConfig, db: &sled::Db) -> anyhow::Result<()> {
    log::debug!("Establishing upstream connection 1");
    let (mut upstream, _) = tokio_tungstenite::connect_async(&config.uri).await?;
    log::debug!("Establishing upstream connection 2");
    for startup_msg in &config.startup_messages {
        upstream.send(WebsocketMessage::Text(serde_json::to_string(startup_msg)?)).await?;
    }
    log::debug!("Establishing upstream connection 3");

    let mut nextkey = get_next_db_key(&db)?;

    let mut handle_msg = |msg: Message| -> anyhow::Result<()> {
        match &msg.stream[..] {
            "listening" => {
                log::info!("Established upstream connection");
            }
            "authorization" => {
                log::debug!("Received 'authorization' response");
            }
            x if x.starts_with("AM.") => {
                log::debug!("Received minutely update for {}", x);
                db.insert(nextkey.to_be_bytes(), serde_json::to_vec(&msg.data)?)?;
                nextkey = nextkey.checked_add(1).context("Key space is full")?;
            }
            x => {
                log::warn!("Strange message from upstram of type {}", x);
            }
        }
        Ok(())
    };

    while let Some(msg) = upstream.next().await  {
        match msg {
            Ok(WebsocketMessage::Text(msg)) => handle_msg(serde_json::from_str(&msg)?)?,
            Ok(WebsocketMessage::Binary(msg)) => handle_msg(serde_json::from_slice(&msg)?)?,
            Ok(WebsocketMessage::Close(c)) => {
                log::warn!("Close message from upstream: {:?}", c);
            }
            Ok(WebsocketMessage::Ping(_)) => {
                log::debug!("WebSocket ping from upstream");
            }
            Ok(_) => {
                log::warn!("other WebSocket message from upstream");
            }
            Err(e) => log::error!("From upstream websocket: {}", e),
        }
    }
    Ok(())
}

#[tokio::main(flavor="current_thread")]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let opts : Opts = argh::from_env();
    let db = sled::open(opts.database)?;

    let config : ClientConfig = serde_json::from_reader(std::io::BufReader::new(std::fs::File::open(opts.client_config)?))?;

    let listener = tokio::net::TcpListener::bind(opts.listen_addr).await?;

    let db_ = db.clone();
    tokio::spawn(async move {
        loop {
            match listener.accept().await  {
                Ok((client_socket, addr)) => {
                    log::info!("Incoming client connection from {}", addr);
                    let db__ = db_.clone();
                    tokio::spawn(async move {
                        if let Err(e) = serve_client(client_socket, &db__).await {
                            log::error!("Error serving client: {}", e);
                        }
                        log::info!("Finished serving client from {}", addr);
                    });
                },
                Err(e) => {
                    log::error!("Listening incoming connections: {}", e);
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });

    loop {
        if let Err(e) = handle_upstream(&config, &db).await {
            log::error!("Handling upstream connection existed with error: {}", e);
        }
        log::info!("Finished upstream connection");
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }
     

    Ok(())
}
