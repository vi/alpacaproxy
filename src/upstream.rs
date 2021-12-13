use std::{sync::Arc, time::Duration};

use anyhow::Context;
use futures::{SinkExt, StreamExt};
#[allow(unused_imports)]
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot::{Receiver as OneshotReceiver, Sender as OneshotSender},
    watch::{Receiver as WatchReceiver, Sender as WatchSender},
};
use tokio::time::Instant;

use tokio_tungstenite::tungstenite::Message as WebsocketMessage;

use crate::mainactor::UpstreamStatus;

impl crate::mainactor::UpstreamStats {
    pub async fn handle_upstream(
        self: Arc<Self>,
        uri: &url::Url,
        startup_messages: &[serde_json::Value],
        automirror: bool,
        db: &crate::database::Db,
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
            let cursor = match db.get_first_last_id()? {
                (_, Some(x)) => x + 1,
                _ => 0,
            };

            use crate::client::ControlMessage;
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

        let mut nextkey = db.get_next_db_key()?;
        let mut slowdown_mode = false;

        let mut handle_msg = |msg: crate::Message| -> anyhow::Result<(bool, bool)> {
            match &msg.tag[..] {
                "success" => {
                    log::info!("Success response from upstream: {}", msg.rest.as_object().map(|x|x.get("msg").map(|y|y.as_str())).flatten().flatten().unwrap_or("?") );
                }
                "error" => {
                    log::error!("Error response from upstream: {:?}", serde_json::to_string(&msg));
                }
                "subscription" => {
                    log::info!("Established upstream connection");
                    self.0.lock().unwrap().status = UpstreamStatus::Connected;
                }
                "preroll_finished" => {
                    log::debug!("Received 'preroll_finished' response");
                    if automirror {
                        self.0.lock().unwrap().status = UpstreamStatus::Connected;
                        log::info!("Finished updating the mirror");
                    }
                }
                "hello" => {
                    log::info!("Established upstream connection with a proxy");
                    if !automirror {
                        self.0.lock().unwrap().status = UpstreamStatus::Connected;
                    }
                }
                "b" => {
                    let val : crate::database::MinutelyData = serde_json::from_value(msg.rest)?;
                    log::debug!("Received minutely update for {}", val.ticker);
                    let mut do_yield = false;
                    let mut do_consider_db_size = false;
                    {
                        let mut stats = self.0.lock().unwrap();
                        stats.last_update = Some(Instant::now());
                        stats.received_tickers += 1;
                        if stats.received_tickers % 50 == 0 {
                            do_yield = true;
                        }
                        if stats.received_tickers % 1000 == 0 {
                            do_consider_db_size = true;
                        }
                    }
                    if let Some(size_cap) = size_cap {
                        if do_consider_db_size {
                            let dbsz = db.get_database_disk_size()?;
                            if dbsz > size_cap * 2 {
                                log::warn!(
                                    "Slowing down reading from upstream due to database overflow"
                                );
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

                    db.insert_entry(nextkey, &val)?;
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
            let mut do_yield = false;
            let mut slowdown_mode = false;
            match msg {
                Ok(WebsocketMessage::Text(wsmsg)) => {
                    let msgs : Vec<crate::Message> = serde_json::from_str(&wsmsg)?;
                    for msg in msgs {
                        let (do_yield_, slowdown_mode_) =  handle_msg(msg)?;
                        do_yield|=do_yield_;
                        slowdown_mode|=slowdown_mode_;
                    }
                },
                Ok(WebsocketMessage::Binary(wsmsg)) => {
                    let msgs : Vec<crate::Message> = serde_json::from_slice(&wsmsg)?;
                    for msg in msgs {
                        let (do_yield_, slowdown_mode_) =  handle_msg(msg)?;
                        do_yield|=do_yield_;
                        slowdown_mode|=slowdown_mode_;
                    }
                },
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
                Err(e) => {
                    log::error!("From upstream websocket: {}", e);
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
