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

pub fn main() -> anyhow::Result<()> {
    if std::env::args().find(|x| x == "--version").is_some() {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    env_logger::init();

    let opts: Opts = argh::from_env();

    let first_config = crate::config::read_config(&opts.client_config)?;
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
            if let Err(e) = crate::database::database_capper(
                db__,
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
    let listen_addr = opts.listen_addr;
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
        opts.client_config,
        &db,
        rx,
        opts.max_database_size,
        watcher_tx,
    ));
    rt.shutdown_timeout(Duration::from_millis(100));
    ret
}
