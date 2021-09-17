use std::convert::TryInto;
use std::sync::Arc;
use std::{net::SocketAddr, path::PathBuf};

use anyhow::Context;
use futures::{sink::SinkExt, stream::StreamExt};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
#[allow(unused_imports)]
use tokio::sync::oneshot::{Receiver as OneshotReceiver, Sender as OneshotSender};
use tokio::time::Instant;

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

#[derive(serde_derive::Serialize, Clone, Copy, Debug)]
#[serde(rename_all = "snake_case")]
enum UpstreamStatus {
    Disabled,
    Paused,
    Connecting,
    Connected,
}

#[derive(serde_derive::Serialize, Debug)]
pub struct SystemStatus {
    database_size: u64,
    first_datum_id: Option<u64>,
    last_datum_id: Option<u64>,
    clients_connected: usize,
    last_ticker_update_ms: Option<u64>,
    upstream_status: UpstreamStatus,
}
#[derive(Debug)]
pub enum ConsoleControl {
    Status(OneshotSender<SystemStatus>),
    Shutdown,
    PauseUpstream,
    ResumeUpstream,
    ClientConnected,
    ClientDisconnected,
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
struct MinutelyData {
    ev: String,

    #[serde(rename = "T")]
    t: String,

    #[serde(flatten)]
    rest: serde_json::Value,
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize, Clone)]
pub struct ClientConfig {
    uri: Option<url::Url>,

    startup_messages: Vec<serde_json::Value>,

    require_password: Option<String>,
}

fn get_next_db_key(db: &sled::Db) -> anyhow::Result<u64> {
    let nextkey = match db.last()? {
        None => 0,
        Some((k, _v)) => {
            let k: [u8; 8] = k.as_ref().try_into()?;
            u64::from_be_bytes(k)
                .checked_add(1)
                .context("Key space is full")?
        }
    };
    Ok(nextkey)
}

fn get_first_last_id(db: &sled::Db) -> anyhow::Result<(Option<u64>,Option<u64>)> {
    let firstkey = match db.first()? {
        None => None,
        Some((k, _v)) => {
            let k: [u8; 8] = k.as_ref().try_into()?;
            Some(u64::from_be_bytes(k))
        }
    };
    let lastkey = match db.last()? {
        None => Some(0),
        Some((k, _v)) => {
            let k: [u8; 8] = k.as_ref().try_into()?;
            Some(u64::from_be_bytes(k))
        }
    };
    Ok((firstkey, lastkey))
}

#[derive(serde_derive::Deserialize)]
#[serde(tag = "stream", content = "data")]
#[serde(rename_all = "snake_case")]
enum ControlMessage {
    Preroll(u64),
    Monitor,
    Filter(Vec<String>),
    RemoveRetainingLastN(u64),
    DatabaseSize,
    Status,
    Shutdown,
    PauseUpstream,
    ResumeUpstream,
    CursorToSpecificId(u64),
    Password(String),
}


struct ServeClient {
    ws: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    db: sled::Db,
    cursor: u64,
    filter: Option<hashbrown::HashSet<smol_str::SmolStr>>,
    console_control: Sender<ConsoleControl>,
    require_password: Option<String>
}

async fn serve_client(client_socket: tokio::net::TcpStream, db: &sled::Db, console_control: Sender<ConsoleControl>, require_password: Option<String>) -> anyhow::Result<()> {
    let ws = tokio_tungstenite::accept_async(client_socket).await?;
    let cursor = get_next_db_key(db)?;
    let cc2 = console_control.clone();
    let c = ServeClient {
        ws,
        db: db.clone(),
        cursor,
        filter: None,
        console_control,
        require_password,
    };
    let _ = cc2.send(ConsoleControl::ClientConnected).await;
    let ret = c.run().await;
    let _ = cc2.send(ConsoleControl::ClientDisconnected).await;
    ret
}


impl ServeClient {
    async fn run(mut self) -> anyhow::Result<()> {
        {
            let buf = serde_json::to_string(&Message {
                stream: "hello".to_owned(),
                data: serde_json::Value::Null,
            })
            .unwrap();
            self.ws.send(WebsocketMessage::Text(buf)).await?;
        }

        while let Some(msg) = self.ws.next().await {
            let cmsg: ControlMessage = match msg {
                Ok(WebsocketMessage::Text(msg)) => match serde_json::from_str(&msg) {
                    Ok(x) => x,
                    Err(e) => {
                        self.err(e.to_string()).await?;
                        continue;
                    }
                },
                Ok(WebsocketMessage::Binary(msg)) => match serde_json::from_slice(&msg) {
                    Ok(x) => x,
                    Err(e) => {
                        self.err(e.to_string()).await?;
                        continue;
                    }
                },
                Ok(WebsocketMessage::Close(c)) => {
                    log::info!("Close message from client: {:?}", c);
                    break;
                }
                Ok(WebsocketMessage::Ping(_)) => {
                    log::debug!("WebSocket ping from client");
                    continue;
                }
                Ok(_) => {
                    log::warn!("other WebSocket message from client");
                    continue;
                }
                Err(e) => {
                    log::error!("From client websocket: {}", e);
                    continue;
                }
            };
            if let Err(e) = self.handle_msg(cmsg).await {
                self.err(e.to_string()).await?;
            }
        }
        Ok(())
    }

    async fn err(&mut self, x: String) -> anyhow::Result<()> {
        log::warn!("Sending error to client: {}", x);
        self.ws
            .send(WebsocketMessage::Text(
                serde_json::to_string(&Message {
                    stream: "error".to_owned(),
                    data: serde_json::Value::String(x),
                })
                .unwrap(),
            ))
            .await?;
        Ok(())
    }

    async fn datum(&mut self, v: sled::IVec, id: u64) -> anyhow::Result<()> {
        let minutely: MinutelyData = match serde_json::from_slice(&v) {
            Ok(x) => x,
            Err(_e) => {
                log::warn!(
                    "Failed to properly deserialize minutely datum number {}",
                    id
                );
                return Ok(());
            }
        };
        if let Some(filt) = &self.filter {
            if !filt.contains(&smol_str::SmolStr::new(&minutely.t)) {
                return Ok(());
            }
        }
        let stream = format!("{}.{}", minutely.ev, minutely.t);
        let msg = Message {
            stream,
            data: serde_json::to_value(minutely)?,
        };
        self.ws
            .send(WebsocketMessage::Text(serde_json::to_string(&msg)?))
            .await?;
        Ok(())
    }

    async fn preroller(&mut self, range: impl Iterator<Item = u64>) -> anyhow::Result<()> {
        for x in range {
            let x: u64 = x;
            match self.db.get(x.to_be_bytes())? {
                Some(v) => {
                    self.datum(v, x).await?;
                }
                None => log::warn!("Missing datum number {}", x),
            }
        }
        Ok(())
    }

    async fn handle_msg(&mut self, msg: ControlMessage) -> anyhow::Result<()> {
        if self.require_password.is_some() && ! matches!(msg, ControlMessage::Password(..)) {
            self.err("Supply a password first".to_owned()).await?;
            return Ok(());
        }
        match msg {
            ControlMessage::Preroll(num) => {
                log::info!("  prerolling {} messages for the client", num);
                let start = self.cursor.saturating_sub(num);
                self.preroller(start..self.cursor).await?;
                {
                    let buf = serde_json::to_string(&Message {
                        stream: "preroll_finished".to_owned(),
                        data: serde_json::Value::Null,
                    })
                    .unwrap();
                    self.ws.send(WebsocketMessage::Text(buf)).await?;
                }
                log::debug!("  prerolling finished");
            }
            ControlMessage::Monitor => {
                log::info!("  streaming messages for the client");
                let mut watcher = self.db.watch_prefix(vec![]);

                // https://github.com/spacejam/sled/issues/1368
                while let Some(evt) = (&mut watcher).await {
                    match evt {
                        sled::Event::Insert { key, value } => {
                            let key: Result<[u8; 8], _> = key.as_ref().try_into();
                            if let Ok(key) = key {
                                let key = u64::from_be_bytes(key);
                                self.preroller(self.cursor..key).await?;
                                self.datum(value, key).await?;
                                self.cursor = key.saturating_add(1);
                            }
                        }
                        sled::Event::Remove { key: _ } => (),
                    }
                }
                log::debug!("  streaming messages finished");
            }
            ControlMessage::RemoveRetainingLastN(num) => {
                log::info!("  retaining {} last samples in the database", num);
                let first = (0u64).to_be_bytes();
                let last = self.cursor.saturating_sub(num).to_be_bytes();
                let mut ctr = 0u64;
                for x in self.db.range(first..last) {
                    if let Ok((key, _val)) = x {
                        match self.db.remove(&key) {
                            Err(e) => log::error!("Error removing entry {:?}: {}", key, e),
                            Ok(None) => (),
                            Ok(Some(_)) => ctr += 1,
                        }
                    }
                    if ctr % 50 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
                {
                    let buf = serde_json::to_string(&Message {
                        stream: "remove_finished".to_owned(),
                        data: serde_json::Value::Number(ctr.into()),
                    })
                    .unwrap();
                    self.ws.send(WebsocketMessage::Text(buf)).await?;
                }
                log::info!("  finished removing {} entries", ctr);
            }
            ControlMessage::DatabaseSize => {
                let buf = serde_json::to_string(&Message {
                    stream: "database_size".to_owned(),
                    data: serde_json::Value::Number(self.db.size_on_disk()?.into()),
                })
                .unwrap();
                self.ws.send(WebsocketMessage::Text(buf)).await?;
            }
            ControlMessage::Filter(a) => {
                self.filter = Some(a.into_iter().map(smol_str::SmolStr::new).collect());
            }
            ControlMessage::Status => {
                let (tx,rx) = tokio::sync::oneshot::channel();
                self.console_control.send(ConsoleControl::Status(tx)).await?;
                let ss = rx.await?;
                self.ws.send(WebsocketMessage::Text(serde_json::to_string(&Message {
                    stream: "stats".to_owned(),
                    data: serde_json::to_value(ss)?,
                })?)).await?;
            }
            ControlMessage::Shutdown => self.console_control.send(ConsoleControl::Shutdown).await?,
            ControlMessage::PauseUpstream => self.console_control.send(ConsoleControl::PauseUpstream).await?,
            ControlMessage::ResumeUpstream => self.console_control.send(ConsoleControl::ResumeUpstream).await?,
            ControlMessage::CursorToSpecificId(newid) => self.cursor = newid,
            ControlMessage::Password(supplied_password) => {
                if let Some(required_password) = self.require_password.take() {
                    if required_password != supplied_password {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        self.err("Invalid password".to_owned()).await?;
                        self.require_password = Some(required_password);
                    } else {
                        // `take` above removed the gate
                    }
                } else {
                    self.err("No password required".to_owned()).await?;
                }
            }
        }
        Ok(())
    }
}

pub struct UpstreamStatsBuffer {
    last_update: Option<Instant>,
    status: UpstreamStatus,
}
pub struct UpstreamStats(std::sync::Mutex<UpstreamStatsBuffer>);

pub async fn main_actor(
    config_filename: PathBuf,
    db: &sled::Db,
    mut rx: Receiver<ConsoleControl>,
) -> anyhow::Result<()> {
    let upstream_stats = Arc::new(UpstreamStats(std::sync::Mutex::new(UpstreamStatsBuffer {
        last_update: None,
        status: UpstreamStatus::Disabled,
    })));

    let mut upstream = Some(tokio::spawn(
        upstream_stats
            .clone()
            .connect_upstream(config_filename.clone(), db.clone()),
    ));

    let mut num_clients = 0usize;

    while let Some(msg) = rx.recv().await {
        match msg {
            ConsoleControl::Shutdown => {
                log::info!("Received shutdown message, exiting.");
                break;
            }
            ConsoleControl::ClientConnected => num_clients += 1,
            ConsoleControl::ClientDisconnected => num_clients -= 1,
            ConsoleControl::Status(tx) => {
                let (first_datum_id, last_datum_id) = get_first_last_id(db)?;
                let stats = upstream_stats.0.lock().unwrap();
                let ss = SystemStatus {
                    database_size: db.size_on_disk()?,
                    clients_connected: num_clients,
                    last_ticker_update_ms: stats.last_update.map(|lu| {
                        Instant::now().duration_since(lu).as_millis() as u64
                    }),
                    upstream_status: stats.status,
                    first_datum_id,
                    last_datum_id,
                };
                drop(stats);
                let _ = tx.send(ss);
            }
            ConsoleControl::PauseUpstream if upstream.is_some() => {
                log::info!("Pausing upstream connector");
                upstream_stats.0.lock().unwrap().status = UpstreamStatus::Paused;
                upstream.as_ref().unwrap().abort();
                upstream = None;
            }
            ConsoleControl::ResumeUpstream if upstream.is_none() => {
                upstream_stats.0.lock().unwrap().status = UpstreamStatus::Connecting;
                upstream = Some(tokio::spawn(
                    upstream_stats
                        .clone()
                        .connect_upstream(config_filename.clone(), db.clone()),
                ));
            }
            ConsoleControl::PauseUpstream | ConsoleControl::ResumeUpstream => {
                log::warn!("Ignored useless PauseUpstream or ResumeUpstream message");
            }
        }
    }
    log::info!("Shutting down");

    Ok(())
}

impl UpstreamStats {
    async fn connect_upstream(self: Arc<Self>, config_filename: PathBuf, db: sled::Db) -> () {
        let config: ClientConfig = match read_config(&config_filename) {
            Ok(x) => x,
            Err(e) => {
                log::error!("Failed to read config file: {}", e);
                return;
            }
        };
        if let Some(uri) = &config.uri {
            loop {
                if let Err(e) = self.clone().handle_upstream(&uri, &config.startup_messages, &db).await {
                    log::error!("Handling upstream connection existed with error: {}", e);
                }
                self.0.lock().unwrap().status = UpstreamStatus::Connecting;
                log::info!("Finished upstream connection");
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        } else {
            self.0.lock().unwrap().status = UpstreamStatus::Disabled;
            log::warn!("Not using any upstream, just waiting endlessly");
            futures::future::pending::<()>().await;
        }
    }

    async fn handle_upstream(
        self: Arc<Self>,
        uri: &url::Url,
        startup_messages: &[serde_json::Value],
        db: &sled::Db,
    ) -> anyhow::Result<()> {
        log::debug!("Establishing upstream connection 1");
        let (mut upstream, _) = tokio_tungstenite::connect_async(uri).await?;
        log::debug!("Establishing upstream connection 2");
        for startup_msg in startup_messages {
            upstream
                .send(WebsocketMessage::Text(serde_json::to_string(startup_msg)?))
                .await?;
        }
        log::debug!("Establishing upstream connection 3");

        let mut nextkey = get_next_db_key(&db)?;

        let mut handle_msg = |msg: Message| -> anyhow::Result<()> {
            match &msg.stream[..] {
                "listening" => {
                    log::info!("Established upstream connection");
                    self.0.lock().unwrap().status = UpstreamStatus::Connected;
                }
                "authorization" => {
                    log::debug!("Received 'authorization' response");
                }
                x if x.starts_with("AM.") => {
                    log::debug!("Received minutely update for {}", x);
                    self.0.lock().unwrap().last_update = Some(Instant::now());
                    db.insert(nextkey.to_be_bytes(), serde_json::to_vec(&msg.data)?)?;
                    nextkey = nextkey.checked_add(1).context("Key space is full")?;
                }
                x => {
                    log::warn!("Strange message from upstram of type {}", x);
                }
            }
            Ok(())
        };

        while let Some(msg) = upstream.next().await {
            match msg {
                Ok(WebsocketMessage::Text(msg)) => handle_msg(serde_json::from_str(&msg)?)?,
                Ok(WebsocketMessage::Binary(msg)) => handle_msg(serde_json::from_slice(&msg)?)?,
                Ok(WebsocketMessage::Close(c)) => {
                    log::warn!("Close message from upstream: {:?}", c);
                    break;
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
}

async fn socket_listener(listen_addr: SocketAddr, db: &sled::Db, console: Sender<ConsoleControl>, require_password: Option<String>) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(listen_addr).await?;

    log::debug!("Created listening socket");

    loop {
        match listener.accept().await {
            Ok((client_socket, addr)) => {
                log::info!("Incoming client connection from {}", addr);
                let db__ = db.clone();
                let consctrl = console.clone();
                let require_password = require_password.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve_client(client_socket, &db__, consctrl, require_password).await {
                        log::error!("Error serving client: {}", e);
                    }
                    log::info!("Finished serving client from {}", addr);
                });
            }
            Err(e) => {
                log::error!("Listening incoming connections: {}", e);
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn read_config(config_file: &std::path::Path) -> anyhow::Result<ClientConfig> {
    Ok(serde_json::from_reader(std::io::BufReader::new(
        std::fs::File::open(config_file)?,
    ))?)
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let opts: Opts = argh::from_env();

    let first_config = read_config(&opts.client_config)?;
    log::debug!("Checked config file");


    let db = sled::Config::default()
        .cache_capacity(1024 * 1024)
        .use_compression(true)
        .compression_factor(1)
        .path(opts.database)
        .open()?;

    log::debug!("Opened the database");

    if db.was_recovered() {
        log::warn!("Database was recovered");
    }

    let rt = tokio::runtime::Builder::new_current_thread().enable_io().enable_time().build()?;

    let (tx, rx) = tokio::sync::mpsc::channel(3);

    let db_ = db.clone();
    let listen_addr = opts.listen_addr;
    rt.spawn(async move {
        if let Err(e) = socket_listener(listen_addr, &db_, tx, first_config.require_password).await {
            log::error!("Socket listener: {}", e);
        }
    });

    let ret = rt.block_on(main_actor(opts.client_config, &db, rx));
    rt.shutdown_timeout(Duration::from_millis(100));
    ret
}
