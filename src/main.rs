#[allow(unused_imports)]
use tokio::sync::oneshot::{Receiver as OneshotReceiver, Sender as OneshotSender};
#[allow(unused_imports)]
use tokio::sync::watch::{Receiver as WatchReceiver, Sender as WatchSender};

pub mod cli;
pub mod client;
pub mod config;
pub mod database;
pub mod mainactor;
pub mod stats;
pub mod metrics;

pub use metrics::METRICS;

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
struct Message {
    #[serde(rename = "T")]
    tag: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<u64>,

    /// Probably contains `MinutelyData`. 
    #[serde(flatten)]
    pub rest: serde_json::Value,
}

fn main() -> anyhow::Result<()> {
    cli::main()
}
