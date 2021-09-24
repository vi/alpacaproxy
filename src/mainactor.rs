use std::{path::PathBuf, sync::Arc, time::Duration};

#[allow(unused_imports)]
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot::{Receiver as OneshotReceiver, Sender as OneshotSender},
    watch::{Receiver as WatchReceiver, Sender as WatchSender},
};
use tokio::time::Instant;

#[path = "upstream.rs"]
pub mod upstream;

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
    WriteConfig(crate::config::ClientConfig),
    ReadConfig(OneshotSender<crate::config::ClientConfig>),
    GetUpstreamState(OneshotSender<UpstreamStatus>),
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

    let mut upstream = Some(tokio::spawn(upstream_stats.clone().connect_upstream(
        config_filename.clone(),
        db.clone(),
        size_cap,
        watcher_sender.clone(),
    )));

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
                let (first_datum_id, last_datum_id) = crate::database::get_first_last_id(db)?;
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
                upstream = Some(tokio::spawn(upstream_stats.clone().connect_upstream(
                    config_filename.clone(),
                    db.clone(),
                    size_cap,
                    watcher_sender.clone(),
                )));
            }
            ConsoleControl::PauseUpstream | ConsoleControl::ResumeUpstream => {
                log::warn!("Ignored useless PauseUpstream or ResumeUpstream message");
            }
            ConsoleControl::WriteConfig(new_config) => {
                crate::config::write_config(&config_filename, &new_config)?;
            }
            ConsoleControl::ReadConfig(tx) => {
                let _ = tx.send(crate::config::read_config(&config_filename)?);
            }
        }
    }
    log::info!("Shutting down");

    Ok(())
}

impl UpstreamStats {
    async fn connect_upstream(
        self: Arc<Self>,
        config_filename: PathBuf,
        db: sled::Db,
        size_cap: Option<u64>,
        watcher_tx: UnboundedSender<u64>,
    ) -> () {
        let config: crate::config::ClientConfig = match crate::config::read_config(&config_filename)
        {
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
                    .handle_upstream(
                        &uri,
                        &config.startup_messages,
                        config.automirror,
                        &db,
                        size_cap,
                        watcher_tx.clone(),
                    )
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

    // see fn handle_upstrean in other file
}
