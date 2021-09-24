#[allow(unused_imports)]
use tokio::sync::oneshot::{Receiver as OneshotReceiver, Sender as OneshotSender};
#[allow(unused_imports)]
use tokio::sync::watch::{Receiver as WatchReceiver, Sender as WatchSender};

pub mod cli;
pub mod client;
pub mod config;
pub mod database;
pub mod mainactor;

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
struct Message {
    stream: String,
    data: serde_json::Value,

    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<u64>,
}

fn main() -> anyhow::Result<()> {
    cli::main()
}
