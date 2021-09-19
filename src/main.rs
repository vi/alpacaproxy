use std::convert::TryInto;
use std::sync::Arc;
use std::{net::SocketAddr, path::PathBuf};

use anyhow::Context;
use futures::{sink::SinkExt, stream::StreamExt};
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
#[allow(unused_imports)]
use tokio::sync::oneshot::{Receiver as OneshotReceiver, Sender as OneshotSender};
#[allow(unused_imports)]
use tokio::sync::watch::{Sender as WatchSender, Receiver as WatchReceiver};
use tokio::time::Instant;

use tokio_tungstenite::tungstenite::Message as WebsocketMessage;

/// Connect to Alpaca and act as a special caching proxy server for it
#[derive(argh::FromArgs)]
struct Opts {
    /// show version and exit
    #[argh(switch)]
    #[allow(dead_code)]
    version: bool,

    #[argh(positional)]
    database: PathBuf,

    /// start removing early entries from database if size exceeds this
    #[argh(option,short='L')]
    max_database_size: Option<u64>,

    /// satabase size scanning interval
    #[argh(option,default="60")]
    database_size_checkup_interval_secs: u64,

    #[argh(positional)]
    client_config: PathBuf,

    #[argh(positional)]
    listen_addr: SocketAddr,
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
struct Message {
    stream: String,
    data: serde_json::Value,

    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<u64>,
}

#[derive(serde_derive::Serialize, Clone, Copy, Debug)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamStatus {
    Disabled,
    Paused,
    Connecting,
    Connected,
    Mirroring,
}

#[derive(serde_derive::Serialize, Debug)]
pub struct SystemStatus {
    database_size: u64,
    first_datum_id: Option<u64>,
    last_datum_id: Option<u64>,
    clients_connected: usize,
    last_ticker_update_ms: Option<u64>,
    upstream_status: UpstreamStatus,
    new_tickers_this_session: usize,
    server_version: String,
}
#[derive(Debug)]
pub enum ConsoleControl {
    Status(OneshotSender<SystemStatus>),
    Shutdown,
    PauseUpstream,
    ResumeUpstream,
    ClientConnected,
    ClientDisconnected,
    WriteConfig(ClientConfig),
    ReadConfig(OneshotSender<ClientConfig>),
    GetUpstreamState(OneshotSender<UpstreamStatus>),
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
struct MinutelyData {
    ev: String,

    #[serde(rename = "T")]
    t: String,

    #[serde(flatten)]
    rest: serde_json::Value,
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize, Clone, Debug)]
pub struct ClientConfig {
    uri: Option<url::Url>,

    startup_messages: Vec<serde_json::Value>,

    require_password: Option<String>,

    automirror: bool,
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

fn get_first_last_id(db: &sled::Db) -> anyhow::Result<(Option<u64>, Option<u64>)> {
    let firstkey = match db.first()? {
        None => None,
        Some((k, _v)) => {
            let k: [u8; 8] = k.as_ref().try_into()?;
            Some(u64::from_be_bytes(k))
        }
    };
    let lastkey = match db.last()? {
        None => None,
        Some((k, _v)) => {
            let k: [u8; 8] = k.as_ref().try_into()?;
            Some(u64::from_be_bytes(k))
        }
    };
    Ok((firstkey, lastkey))
}

#[derive(serde_derive::Deserialize, serde_derive::Serialize)]
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
    WriteConfig(ClientConfig),
    ReadConfig,
    PleaseIncludeIds,
}

struct ServeClient {
    ws: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    db: sled::Db,
    cursor: Option<u64>,
    filter: Option<hashbrown::HashSet<smol_str::SmolStr>>,
    console_control: UnboundedSender<ConsoleControl>,
    require_password: Option<String>,
    db_watcher: WatchReceiver<u64>,
    include_ids: bool,
}

async fn serve_client(
    client_socket: tokio::net::TcpStream,
    db: &sled::Db,
    console_control: UnboundedSender<ConsoleControl>,
    require_password: Option<String>,
    db_watcher: WatchReceiver<u64>,
) -> anyhow::Result<()> {
    let ws = tokio_tungstenite::accept_async(client_socket).await?;
    let cc2 = console_control.clone();
    let c = ServeClient {
        ws,
        db: db.clone(),
        cursor: None,
        filter: None,
        console_control,
        require_password,
        db_watcher,
        include_ids: false,
    };
    let _ = cc2.send(ConsoleControl::ClientConnected);
    let ret = c.run().await;
    let _ = cc2.send(ConsoleControl::ClientDisconnected);
    ret
}

impl ServeClient {
    async fn get_cursor(&mut self) -> anyhow::Result<u64> {
        if let Some(c) = self.cursor {
            return Ok(c);
        }
        loop {
            let (tx,rx) = tokio::sync::oneshot::channel();
            self.console_control.send(ConsoleControl::GetUpstreamState(tx))?;
            let upstream_status = rx.await?;
            match upstream_status {
                UpstreamStatus::Disabled => break,
                UpstreamStatus::Paused => break,
                UpstreamStatus::Connecting => (),
                UpstreamStatus::Connected => break,
                UpstreamStatus::Mirroring => (),
            }
            log::warn!("  waiting for proper status before initializing cursor, now {:?}", upstream_status);
            tokio::time::sleep(Duration::from_millis(2000)).await;
        }
        self.cursor = Some(get_next_db_key(&self.db)?);
        Ok(self.cursor.unwrap())
    }

    async fn run(mut self) -> anyhow::Result<()> {
        let upstream_status;
        {
            let (tx,rx) = tokio::sync::oneshot::channel();
            self.console_control.send(ConsoleControl::GetUpstreamState(tx))?;
            upstream_status = rx.await?;
        }

        {
            let buf = serde_json::to_string(&Message {
                stream: "hello".to_owned(),
                data: serde_json::to_value(upstream_status)?,
                id: None,
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
                    log::info!("  from client websocket: {}", e);
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
                    id: None,
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
            id: if self.include_ids { Some(id) } else { None }, 
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
        if self.require_password.is_some() && !matches!(msg, ControlMessage::Password(..)) {
            self.err("Supply a password first".to_owned()).await?;
            return Ok(());
        }
        match msg {
            ControlMessage::Preroll(num) => {
                let cursor = self.get_cursor().await?;
                let start = cursor.saturating_sub(num);
                let rangeend = get_first_last_id(&self.db)?.1.unwrap_or(cursor);
                let range = start..=rangeend;
                log::info!("  prerolling {} messages for the client, really {}", num, rangeend+1-start);
                self.preroller(range).await?;
                {
                    let buf = serde_json::to_string(&Message {
                        stream: "preroll_finished".to_owned(),
                        data: serde_json::Value::Null,
                        id: None,
                    })
                    .unwrap();
                    self.ws.send(WebsocketMessage::Text(buf)).await?;
                }
                log::debug!("  prerolling finished");
                self.cursor = Some(rangeend+1);
            }
            ControlMessage::Monitor => {
                let _ = self.get_cursor().await?;
                log::info!("  streaming messages for the client");

                loop {
                    self.db_watcher.changed().await?;
                    let key = *self.db_watcher.borrow_and_update();
                    let cursor = self.cursor.unwrap();
                    let range = cursor..=key;
                    log::debug!("    range {:?}", range);
                    if ! range.is_empty() {
                        self.preroller(cursor..=key).await?;
                        self.cursor = Some(key.saturating_add(1));
                    }
                }
            }
            ControlMessage::RemoveRetainingLastN(num) => {
                log::info!("  retaining {} last samples in the database", num);
                let first = (0u64).to_be_bytes();
                let cursor = self.get_cursor().await?;
                let last = cursor.saturating_sub(num).to_be_bytes();
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
                        id: None,
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
                    id: None,
                })
                .unwrap();
                self.ws.send(WebsocketMessage::Text(buf)).await?;
            }
            ControlMessage::Filter(a) => {
                self.filter = Some(a.into_iter().map(smol_str::SmolStr::new).collect());
            }
            ControlMessage::Status => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                self.console_control
                    .send(ConsoleControl::Status(tx))?;
                let ss = rx.await?;
                self.ws
                    .send(WebsocketMessage::Text(serde_json::to_string(&Message {
                        stream: "stats".to_owned(),
                        data: serde_json::to_value(ss)?,
                        id: None,
                    })?))
                    .await?;
            }
            ControlMessage::Shutdown => self.console_control.send(ConsoleControl::Shutdown)?,
            ControlMessage::PauseUpstream => {
                self.console_control
                    .send(ConsoleControl::PauseUpstream)?
            }
            ControlMessage::ResumeUpstream => {
                self.console_control
                    .send(ConsoleControl::ResumeUpstream)?
            }
            ControlMessage::CursorToSpecificId(newid) => {
                log::info!("  explicit cursor set to {}", newid);
                self.cursor = Some(newid);
            }
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
            ControlMessage::WriteConfig(new_config) => {
                self.console_control
                    .send(ConsoleControl::WriteConfig(new_config))?
            }
            ControlMessage::ReadConfig => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                self.console_control
                    .send(ConsoleControl::ReadConfig(tx))?;
                let cc = rx.await?;
                self.ws
                    .send(WebsocketMessage::Text(serde_json::to_string(&Message {
                        stream: "config".to_owned(),
                        data: serde_json::to_value(cc)?,
                        id: None,
                    })?))
                    .await?;
            }
            ControlMessage::PleaseIncludeIds => {
                self.include_ids = true;
            }
        }
        Ok(())
    }
}

pub struct UpstreamStatsBuffer {
    last_update: Option<Instant>,
    status: UpstreamStatus,
    received_tickers: usize,
}
pub struct UpstreamStats(std::sync::Mutex<UpstreamStatsBuffer>);

pub async fn main_actor(
    config_filename: PathBuf,
    db: &sled::Db,
    mut rx: UnboundedReceiver<ConsoleControl>,
    size_cap: Option<u64>,
    watcher_tx: WatchSender<u64>,
) -> anyhow::Result<()> {
    let upstream_stats = Arc::new(UpstreamStats(std::sync::Mutex::new(UpstreamStatsBuffer {
        last_update: None,
        status: UpstreamStatus::Disabled,
        received_tickers: 0,
    })));

    // converter between WatchSender and UnboundedSender
    let (watcher_sender, mut watcher_receiver) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(msg) = watcher_receiver.recv().await {
            if watcher_tx.send(msg).is_err() {
                break;
            }
        }
    });

    let mut upstream = Some(tokio::spawn(
        upstream_stats
            .clone()
            .connect_upstream(config_filename.clone(), db.clone(), size_cap, watcher_sender.clone()),
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
                    last_ticker_update_ms: stats
                        .last_update
                        .map(|lu| Instant::now().duration_since(lu).as_millis() as u64),
                    upstream_status: stats.status,
                    first_datum_id,
                    last_datum_id,
                    new_tickers_this_session: stats.received_tickers,
                    server_version: env!("CARGO_PKG_VERSION").to_owned(),
                };
                drop(stats);
                let _ = tx.send(ss);
            }
            ConsoleControl::GetUpstreamState(tx) => {
                let stats = upstream_stats.0.lock().unwrap();
                let ss = stats.status;
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
                        .connect_upstream(config_filename.clone(), db.clone(), size_cap, watcher_sender.clone()),
                ));
            }
            ConsoleControl::PauseUpstream | ConsoleControl::ResumeUpstream => {
                log::warn!("Ignored useless PauseUpstream or ResumeUpstream message");
            }
            ConsoleControl::WriteConfig(new_config) => {
                write_config(&config_filename, &new_config)?;
            }
            ConsoleControl::ReadConfig(tx) => {
                let _ = tx.send(read_config(&config_filename)?);
            }
        }
    }
    log::info!("Shutting down");

    Ok(())
}

impl UpstreamStats {
    async fn connect_upstream(self: Arc<Self>, config_filename: PathBuf, db: sled::Db, size_cap: Option<u64>, watcher_tx: UnboundedSender<u64>) -> () {
        let config: ClientConfig = match read_config(&config_filename) {
            Ok(x) => x,
            Err(e) => {
                log::error!("Failed to read config file: {}", e);
                return;
            }
        };
        if let Some(uri) = &config.uri {
            loop {
                if let Err(e) = self
                    .clone()
                    .handle_upstream(&uri, &config.startup_messages, config.automirror, &db, size_cap, watcher_tx.clone())
                    .await
                {
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
        automirror: bool,
        db: &sled::Db,
        size_cap: Option<u64>,
        watcher_tx: UnboundedSender<u64>,
    ) -> anyhow::Result<()> {
        log::debug!("Establishing upstream connection 1");
        let (mut upstream, _) = tokio_tungstenite::connect_async(uri).await?;
        log::debug!("Establishing upstream connection 2");
        for startup_msg in startup_messages {
            upstream
                .send(WebsocketMessage::Text(serde_json::to_string(startup_msg)?))
                .await?;
        }
        if automirror {
            self.0.lock().unwrap().status = UpstreamStatus::Mirroring;
            let cursor = match get_first_last_id(db)? {
                (_, Some(x)) => x+1,
                _ => 0,
            };
            upstream
                .send(WebsocketMessage::Text(serde_json::to_string(
                    &ControlMessage::PleaseIncludeIds,
                )?))
                .await?;
            upstream
                .send(WebsocketMessage::Text(serde_json::to_string(
                    &ControlMessage::CursorToSpecificId(cursor),
                )?))
                .await?;
            upstream
                .send(WebsocketMessage::Text(serde_json::to_string(
                    &ControlMessage::Preroll(0),
                )?))
                .await?;
            upstream
                .send(WebsocketMessage::Text(serde_json::to_string(
                    &ControlMessage::Monitor,
                )?))
                .await?;
        }
        log::debug!("Establishing upstream connection 3");

        let mut nextkey = get_next_db_key(&db)?;
        let mut slowdown_mode = false;

        let mut handle_msg = |msg: Message| -> anyhow::Result<(bool, bool)> {
            match &msg.stream[..] {
                "listening" => {
                    log::info!("Established upstream connection");
                    self.0.lock().unwrap().status = UpstreamStatus::Connected;
                }
                "authorization" => {
                    log::debug!("Received 'authorization' response");
                }
                "preroll_finished" => {
                    log::debug!("Received 'preroll_finished' response");
                    if automirror {
                        self.0.lock().unwrap().status = UpstreamStatus::Connected;
                    }
                }
                "hello" => {
                    log::info!("Established upstream connection with a proxy");
                    if !automirror {
                        self.0.lock().unwrap().status = UpstreamStatus::Connected;
                    }
                }
                x if x.starts_with("AM.") => {
                    log::debug!("Received minutely update for {}", x);
                    let mut do_yield = false;
                    let mut do_consider_db_size = false;
                    {
                        let mut stats = self.0.lock().unwrap();
                        stats.last_update = Some(Instant::now());
                        stats.received_tickers+=1;
                        if stats.received_tickers % 50 == 0 {
                            do_yield = true;
                        }
                        if stats.received_tickers % 1000 == 0 {
                            do_consider_db_size = true;
                        }
                    }
                    if let Some(size_cap) = size_cap {
                        if do_consider_db_size {
                            let dbsz = db.size_on_disk()?;
                            if dbsz > size_cap*2 {
                                log::warn!("Slowing down reading from upstream due to database overflow");
                                slowdown_mode = true;
                            } else {
                                if slowdown_mode {
                                    log::info!("No longer slowing down reads");
                                }
                                slowdown_mode = false;
                            }
                        }
                    }
                    if let Some(newid) = msg.id {
                        nextkey = newid;
                    } 
                    
                    db.insert(nextkey.to_be_bytes(), serde_json::to_vec(&msg.data)?)?;
                    let _ = watcher_tx.send(nextkey);
                    nextkey = nextkey.checked_add(1).context("Key space is full")?;
                    return Ok((do_yield, slowdown_mode)); 
                }
                x => {
                    log::warn!("Strange message from upstram of type {}", x);
                }
            }
            Ok((false, slowdown_mode))
        };

        while let Some(msg) = upstream.next().await {
            let (do_yield, slowdown_mode) = match msg {
                Ok(WebsocketMessage::Text(msg)) => handle_msg(serde_json::from_str(&msg)?)?,
                Ok(WebsocketMessage::Binary(msg)) => handle_msg(serde_json::from_slice(&msg)?)?,
                Ok(WebsocketMessage::Close(c)) => {
                    log::warn!("Close message from upstream: {:?}", c);
                    break;
                }
                Ok(WebsocketMessage::Ping(_)) => {
                    log::debug!("WebSocket ping from upstream");
                    (false, false)
                }
                Ok(_) => {
                    log::warn!("other WebSocket message from upstream");
                    (false, false)
                }
                Err(e) => {
                    log::error!("From upstream websocket: {}", e);
                    (false, false)
                }
            };
            if do_yield {
                tokio::task::yield_now().await;
            }
            if slowdown_mode {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        Ok(())
    }
}

async fn socket_listener(
    listen_addr: SocketAddr,
    db: &sled::Db,
    console: UnboundedSender<ConsoleControl>,
    require_password: Option<String>,
    watcher_rx: WatchReceiver<u64>
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(listen_addr).await?;

    log::debug!("Created listening socket");

    loop {
        match listener.accept().await {
            Ok((client_socket, addr)) => {
                log::info!("Incoming client connection from {}", addr);
                let db__ = db.clone();
                let consctrl = console.clone();
                let require_password = require_password.clone();
                let db_watcher = watcher_rx.clone();
                tokio::spawn(async move {
                    if let Err(e) =
                        serve_client(client_socket, &db__, consctrl, require_password, db_watcher).await
                    {
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

fn write_config(config_file: &std::path::Path, config: &ClientConfig) -> anyhow::Result<()> {
    serde_json::to_writer_pretty(
        std::io::BufWriter::new(std::fs::File::create(config_file)?),
        config,
    )?;
    Ok(())
}

pub async fn database_capper(db: sled::Db, size_cap: u64, database_size_checkup_interval_secs: u64) -> anyhow::Result<()> {
    let mut cached_db_size = 0;
    loop {
        let dbsz = db.size_on_disk()?;
        log::debug!("Size capper db size report: {}", dbsz);
        if dbsz > size_cap && dbsz != cached_db_size {
            log::debug!("Considering database trimmer");
            let (minid, maxid) = match get_first_last_id(&db)? {
                (Some(a), Some(b)) => (a,b),
                _ => continue,
            };
            let mut len = maxid - minid;
            if len > 10000 {
                log::info!("Triggering database trimmer");
            } else {
                continue;
            }
            len = (len as f32 * (1.0 - size_cap as f32 / dbsz as f32)) as u64; 
            for k in minid..(minid+len) {
                let _ =db.remove(k.to_be_bytes())?;
                if k % 100 == 0 {
                    tokio::task::yield_now().await;
                }
            }
            let _ = db.flush_async().await;
            log::debug!("Trimmer run finished");
            tokio::time::sleep(Duration::new(database_size_checkup_interval_secs, 0)).await;
            cached_db_size = dbsz;
        } else {
            cached_db_size = dbsz;
            tokio::time::sleep(Duration::new(database_size_checkup_interval_secs, 0)).await;
        }
    }
}

fn main() -> anyhow::Result<()> {
    if std::env::args().find(|x|x=="--version").is_some() {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
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

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()?;


    let (watcher_tx, watcher_rx) = tokio::sync::watch::channel(0);


    if let Some(size_cap) = opts.max_database_size {
        let db__ = db.clone();  
        let database_size_checkup_interval_secs = opts.database_size_checkup_interval_secs;
        rt.spawn(async move {
            if let Err(e) = database_capper(db__, size_cap, database_size_checkup_interval_secs).await {
                log::error!("Database capper: {}", e);
                log::error!("Database capper errors are critical. Exiting.");
                std::process::exit(33);
            }
        });
    }

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let db_ = db.clone();
    let listen_addr = opts.listen_addr;
    rt.spawn(async move {
        if let Err(e) = socket_listener(listen_addr, &db_, tx, first_config.require_password, watcher_rx).await
        {
            log::error!("Socket listener: {}", e);
        }
    });

    let ret = rt.block_on(main_actor(opts.client_config, &db, rx, opts.max_database_size, watcher_tx));
    rt.shutdown_timeout(Duration::from_millis(100));
    ret
}
