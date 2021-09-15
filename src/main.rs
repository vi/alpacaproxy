use std::convert::TryInto;
use std::{net::SocketAddr, path::PathBuf};

use std::time::Duration;
use anyhow::Context;
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
    data: serde_json::Map<String, serde_json::Value>,
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
struct ClientConfig {
    #[serde(with = "http_serde::uri")]
    uri: http::Uri,

    startup_messages: Vec<serde_json::Value>,

    preroll_messages: u64,
}

async fn serve_client(mut client_socket: tokio::net::TcpStream) -> anyhow::Result<()> {
    let mut client = tokio_tungstenite::accept_async(client_socket).await?;
    {
        let mut buf = serde_json::to_string(&Message{stream:"hello".to_owned(), data:serde_json::Map::new()}).unwrap();
        client.send(WebsocketMessage::Text(buf)).await?;
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

    let mut nextkey = match db.last()? {
        None => 0,
        Some((k, _v)) => {
            let k : [u8; 8] = k.as_ref().try_into()?;
            u64::from_be_bytes(k).checked_add(1).context("Key space is full")?
        }
    };

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

    tokio::spawn(async move {
        loop {
            match listener.accept().await  {
                Ok((client_socket, addr)) => {
                    log::info!("Incoming client connection from {}", addr);
                    tokio::spawn(async move {
                        if let Err(e) = serve_client(client_socket).await {
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
