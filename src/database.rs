use std::{convert::TryInto, time::Duration};

use anyhow::Context;

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
pub struct MinutelyData {
    pub ev: String,

    #[serde(rename = "T")]
    pub t: String,

    #[serde(flatten)]
    pub rest: serde_json::Value,
}

pub fn get_next_db_key(db: &sled::Db) -> anyhow::Result<u64> {
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

pub fn get_first_last_id(db: &sled::Db) -> anyhow::Result<(Option<u64>, Option<u64>)> {
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

pub async fn database_capper(
    db: sled::Db,
    size_cap: u64,
    database_size_checkup_interval_secs: u64,
) -> anyhow::Result<()> {
    let mut cached_db_size = 0;
    loop {
        let dbsz = db.size_on_disk()?;
        log::debug!("Size capper db size report: {}", dbsz);
        if dbsz > size_cap && dbsz != cached_db_size {
            log::debug!("Considering database trimmer");
            let (minid, maxid) = match get_first_last_id(&db)? {
                (Some(a), Some(b)) => (a, b),
                _ => continue,
            };
            let mut len = maxid - minid;
            if len > 10000 {
                log::info!("Triggering database trimmer");
            } else {
                continue;
            }
            len = (len as f32 * (1.0 - size_cap as f32 / dbsz as f32)) as u64;
            for k in minid..(minid + len) {
                let _ = db.remove(k.to_be_bytes())?;
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
