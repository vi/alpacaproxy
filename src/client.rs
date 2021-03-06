use std::{net::SocketAddr, time::Duration};

use crate::{database::MinutelyData, mainactor::ConsoleControl, mainactor::UpstreamStatus};
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message as WebsocketMessage;

#[allow(unused_imports)]
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot::{Receiver as OneshotReceiver, Sender as OneshotSender},
    watch::{Receiver as WatchReceiver, Sender as WatchSender},
};

#[derive(serde_derive::Deserialize, serde_derive::Serialize)]
#[serde(tag = "action")]
#[serde(rename_all = "snake_case")]
pub enum ControlMessage {
    Preroll { data: u64 },
    Monitor,
    Filter { data: Vec<String> },
    RemoveRetainingLastN { data: u64 },
    DatabaseSize,
    Status,
    Shutdown,
    PauseUpstream,
    ResumeUpstream,
    CursorToSpecificId { data: u64 },
    Password { data: String },
    WriteConfig { data: crate::config::ClientConfig },
    ReadConfig,
    PleaseIncludeIds,
}

struct ServeClient {
    ws: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    db: crate::database::Db,
    cursor: Option<u64>,
    filter: Option<hashbrown::HashSet<smol_str::SmolStr>>,
    console_control: UnboundedSender<ConsoleControl>,
    require_password: Option<String>,
    db_watcher: WatchReceiver<u64>,
    include_ids: bool,
    now_streaming: bool,
}

async fn serve_client(
    client_socket: tokio::net::TcpStream,
    db: &crate::database::Db,
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
        now_streaming: false,
    };
    let _ = cc2.send(ConsoleControl::ClientConnected);
    let _g = crate::METRICS.client_session_durations.start_timer();
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
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.console_control
                .send(ConsoleControl::GetUpstreamState(tx))?;
            let upstream_status = rx.await?;
            match upstream_status {
                UpstreamStatus::Disabled => break,
                UpstreamStatus::Paused => break,
                UpstreamStatus::Connecting => (),
                UpstreamStatus::Connected => break,
                UpstreamStatus::Mirroring => (),
            }
            log::warn!(
                "  waiting for proper status before initializing cursor, now {:?}",
                upstream_status
            );
            tokio::time::sleep(Duration::from_millis(2000)).await;
        }
        self.cursor = Some(self.db.get_next_db_key()?);
        Ok(self.cursor.unwrap())
    }

    async fn run(mut self) -> anyhow::Result<()> {
        let upstream_status;
        {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.console_control
                .send(ConsoleControl::GetUpstreamState(tx))?;
            upstream_status = rx.await?;
        }

        {
            let buf = serde_json::to_string(&vec![crate::Message {
                tag: "hello".to_owned(),
                rest: serde_json::json!({"status": serde_json::to_value(upstream_status)?}),
                id: None,
            }])
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
        use serde_json::value::{Map, Value};
        use serde_json::Number;
        let mut error = Map::with_capacity(3);
        error.insert("T".to_owned(), Value::String("error".to_owned()));
        error.insert("code".to_owned(), Value::Number(Number::from(505)));
        error.insert("msg".to_owned(), Value::String(x));
        self.ws
            .send(WebsocketMessage::Text(
                serde_json::to_string(&Value::Array(vec![Value::Object(error)])).unwrap(),
            ))
            .await?;
        Ok(())
    }

    async fn datum(&mut self, minutely: MinutelyData, id: u64) -> anyhow::Result<()> {
        if let Some(filt) = &self.filter {
            if !filt.contains(&smol_str::SmolStr::new(&minutely.ticker)) {
                return Ok(());
            }
        }
        let msg = crate::Message {
            tag: "b".to_owned(),
            rest: serde_json::to_value(minutely)?,
            id: if self.include_ids { Some(id) } else { None },
        };

        let _g = if self.now_streaming {
            crate::METRICS.entries_streamed_to_clients.inc();
            crate::METRICS.client_backpressure_stream.start_timer()
        } else {
            crate::METRICS.entries_prerolled_to_clients.inc();
            crate::METRICS.client_backpressure_preroll.start_timer()
        };
        self.ws
            .send(WebsocketMessage::Text(serde_json::to_string(&vec![msg])?))
            .await?;
        Ok(())
    }

    async fn preroller(&mut self, range: impl Iterator<Item = u64>) -> anyhow::Result<()> {
        if let Some(ref filter) = self.filter {
            if let Some(mut fr) = self.db.get_filtered_reader(filter) {
                log::debug!("Obtained a indexed/filtered reader");
                for x in range {
                    if x % 2000 == 0 {
                        tokio::task::yield_now().await;
                    }
                    let x: u64 = x;
                    if let Some(v) = fr.get_entry_by_id_if_matches(x)? {
                        self.datum(v, x).await?;
                    }
                }
                return Ok(());
            } else {
                log::debug!("Failed to obtain indexed/filtered reader");
            }
        }
        for x in range {
            let x: u64 = x;
            if x % 2000 == 0 {
                tokio::task::yield_now().await;
            }
            match self.db.get_entry_by_id(x)? {
                Some(v) => {
                    self.datum(v, x).await?;
                }
                None => log::warn!("Missing datum number {}", x),
            }
        }
        Ok(())
    }

    async fn handle_msg(&mut self, msg: ControlMessage) -> anyhow::Result<()> {
        if self.require_password.is_some() && !matches!(msg, ControlMessage::Password { .. }) {
            self.err("Supply a password first".to_owned()).await?;
            return Ok(());
        }
        match msg {
            ControlMessage::Preroll { data: num } => {
                let cursor = self.get_cursor().await?;
                let mut start = cursor.saturating_sub(num);
                let (first, last) = self.db.get_first_last_id()?;
                if let Some(first) = first {
                    start = start.max(first);
                }
                let rangeend = last.unwrap_or(cursor);
                let range = start..=rangeend;
                log::info!(
                    "  prerolling {} messages for the client, really {}",
                    num,
                    rangeend + 1 - start
                );
                crate::METRICS.client_preroll_commands.inc();
                let _g = crate::METRICS.preroll_durations.start_timer();
                self.preroller(range).await?;
                {
                    let buf = serde_json::to_string(&vec![crate::Message {
                        tag: "preroll_finished".to_owned(),
                        rest: serde_json::Value::Null,
                        id: None,
                    }])
                    .unwrap();
                    self.ws.send(WebsocketMessage::Text(buf)).await?;
                }
                log::debug!("  prerolling finished");
                self.cursor = Some(rangeend + 1);
            }
            ControlMessage::Monitor => {
                let _ = self.get_cursor().await?;
                log::info!("  streaming messages for the client");
                crate::METRICS.client_monitor_comands.inc();
                self.now_streaming = true;

                loop {
                    self.db_watcher.changed().await?;
                    let key = *self.db_watcher.borrow_and_update();
                    let cursor = self.cursor.unwrap();
                    let range = cursor..=key;
                    log::debug!("    range {:?}", range);
                    if !range.is_empty() {
                        self.preroller(cursor..=key).await?;
                        self.cursor = Some(key.saturating_add(1));
                    }
                }
            }
            ControlMessage::RemoveRetainingLastN { data: num } => {
                log::info!("  retaining {} last samples in the database", num);
                let first = self.db.get_first_last_id()?.0.unwrap_or(0);
                let cursor = self.get_cursor().await?;
                let last = cursor.saturating_sub(num);
                let mut ctr = 0u64;
                for key in first..last {
                    match self.db.remove_entry(key) {
                        Err(e) => log::error!("Error removing entry {:?}: {}", key, e),
                        Ok(false) => (),
                        Ok(true) => ctr += 1,
                    }
                    if ctr % 50 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
                {
                    let buf = serde_json::to_string(&vec![crate::Message {
                        tag: "remove_finished".to_owned(),
                        rest: serde_json::Value::Number(ctr.into()),
                        id: None,
                    }])
                    .unwrap();
                    self.ws.send(WebsocketMessage::Text(buf)).await?;
                }
                log::info!("  finished removing {} entries", ctr);
            }
            ControlMessage::DatabaseSize => {
                let buf = serde_json::to_string(&vec![crate::Message {
                    tag: "database_size".to_owned(),
                    rest: serde_json::Value::Number(self.db.get_database_disk_size()?.into()),
                    id: None,
                }])
                .unwrap();
                self.ws.send(WebsocketMessage::Text(buf)).await?;
            }
            ControlMessage::Filter { data: a } => {
                self.filter = Some(a.into_iter().map(smol_str::SmolStr::new).collect());
            }
            ControlMessage::Status => {
                crate::METRICS.client_status_queries.inc();
                let (tx, rx) = tokio::sync::oneshot::channel();
                self.console_control.send(ConsoleControl::Status(tx))?;
                let ss = rx.await?;
                self.ws
                    .send(WebsocketMessage::Text(serde_json::to_string(&vec![
                        crate::Message {
                            tag: "stats".to_owned(),
                            rest: serde_json::to_value(ss)?,
                            id: None,
                        },
                    ])?))
                    .await?;
            }
            ControlMessage::Shutdown => self.console_control.send(ConsoleControl::Shutdown)?,
            ControlMessage::PauseUpstream => {
                self.console_control.send(ConsoleControl::PauseUpstream)?
            }
            ControlMessage::ResumeUpstream => {
                self.console_control.send(ConsoleControl::ResumeUpstream)?
            }
            ControlMessage::CursorToSpecificId { data: newid } => {
                log::info!("  explicit cursor set to {}", newid);
                self.cursor = Some(newid);
            }
            ControlMessage::Password {
                data: supplied_password,
            } => {
                if let Some(required_password) = self.require_password.take() {
                    if required_password != supplied_password {
                        crate::METRICS.client_invalid_password.inc();
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
            ControlMessage::WriteConfig { data: new_config } => {
                crate::METRICS.client_settings_writes.inc();
                self.console_control
                    .send(ConsoleControl::WriteConfig(new_config))?
            }
            ControlMessage::ReadConfig => {
                crate::METRICS.client_settings_reads.inc();
                let (tx, rx) = tokio::sync::oneshot::channel();
                self.console_control.send(ConsoleControl::ReadConfig(tx))?;
                let cc = rx.await?;
                self.ws
                    .send(WebsocketMessage::Text(serde_json::to_string(&vec![
                        crate::Message {
                            tag: "config".to_owned(),
                            rest: serde_json::to_value(cc)?,
                            id: None,
                        },
                    ])?))
                    .await?;
            }
            ControlMessage::PleaseIncludeIds => {
                self.include_ids = true;
            }
        }
        Ok(())
    }
}

pub async fn socket_listener(
    listen_addr: SocketAddr,
    db: &crate::database::Db,
    console: UnboundedSender<ConsoleControl>,
    require_password: Option<String>,
    watcher_rx: WatchReceiver<u64>,
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
                    use crate::metrics::guarded::GuardedGauge;
                    crate::METRICS.client_connects.inc();
                    let _g = crate::METRICS.clients_connected.guarded_inc();
                    if let Err(e) =
                        serve_client(client_socket, &db__, consctrl, require_password, db_watcher)
                            .await
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
