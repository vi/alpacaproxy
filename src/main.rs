use std::{net::SocketAddr, path::PathBuf};

use tokio::io::AsyncWriteExt;

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
}

async fn serve_client(mut client_socket: tokio::net::TcpStream) -> anyhow::Result<()> {
    client_socket.write_all(&serde_json::to_vec(&Message{stream:"hello".to_owned(), data:serde_json::Map::new()}).unwrap()).await?;
    Ok(())
}

#[tokio::main(flavor="current_thread")]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let opts : Opts = argh::from_env();
    let db = sled::open(opts.database)?;

    let config = serde_json::from_reader(std::io::BufReader::new(std::fs::File::open(opts.client_config)?))?;

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
            tokio::time::sleep(std::time::Duration::from_millis(20));
        }
    });

    Ok(())
}
