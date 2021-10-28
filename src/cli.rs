use std::{net::SocketAddr, path::PathBuf, time::Duration};

/// Connect to Alpaca and act as a special caching proxy server for it
#[derive(argh::FromArgs)]
pub struct Opts {
    /// show version and exit
    #[argh(switch)]
    #[allow(dead_code)]
    pub version: bool,

    #[argh(positional)]
    pub database: PathBuf,

    #[argh(subcommand)]
    pub cmd: Cmd,
}

#[derive(argh::FromArgs)]
#[argh(subcommand)]
pub enum Cmd {
    Serve(Serve),
}

/// Serve data from database, optionally connecting to Alpaca (or another proxy) upstream
#[derive(argh::FromArgs)]
#[argh(subcommand, name="serve")]
pub struct Serve {
    /// start removing early entries from database if size exceeds this
    #[argh(option, short = 'L')]
    pub max_database_size: Option<u64>,

    /// satabase size scanning interval
    #[argh(option, default = "60")]
    pub database_size_checkup_interval_secs: u64,

    #[argh(positional)]
    pub client_config: PathBuf,

    #[argh(positional)]
    pub listen_addr: SocketAddr,
} 

fn opendb(path: &std::path::Path) -> anyhow::Result<sled::Db> {
    Ok(sled::Config::default()
        .cache_capacity(1024 * 1024)
        .use_compression(true)
        .compression_factor(1)
        .path(path)
        .open()?)
}

pub fn main() -> anyhow::Result<()> {
    if std::env::args().find(|x| x == "--version").is_some() {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    env_logger::init();

    let opts: Opts = argh::from_env();

    match opts.cmd {
        Cmd::Serve(servopts) => {
            let first_config = crate::config::read_config(&servopts.client_config)?;
            log::debug!("Checked config file");

            let sled_db = opendb(&opts.database)?;

            log::debug!("Opened the database");

            if sled_db.was_recovered() {
                log::warn!("Database was recovered");
            }
            let db = crate::database::open_sled(sled_db.clone())?;

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()?;

            let (watcher_tx, watcher_rx) = tokio::sync::watch::channel(0);

            if let Some(size_cap) = servopts.max_database_size {
                let database_size_checkup_interval_secs = servopts.database_size_checkup_interval_secs;
                rt.spawn(async move {
                    if let Err(e) = crate::database::database_capper(
                        sled_db,
                        size_cap,
                        database_size_checkup_interval_secs,
                    )
                    .await
                    {
                        log::error!("Database capper: {}", e);
                        log::error!("Database capper errors are critical. Exiting.");
                        std::process::exit(33);
                    }
                });
            }

            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

            let db_ = db.clone();
            let listen_addr = servopts.listen_addr;
            rt.spawn(async move {
                if let Err(e) = crate::client::socket_listener(
                    listen_addr,
                    &db_,
                    tx,
                    first_config.require_password,
                    watcher_rx,
                )
                .await
                {
                    log::error!("Socket listener: {}", e);
                }
            });

            let ret = rt.block_on(crate::mainactor::main_actor(
                servopts.client_config,
                &db,
                rx,
                servopts.max_database_size,
                watcher_tx,
            ));
            rt.shutdown_timeout(Duration::from_millis(100));
            ret
        }
    }

}
