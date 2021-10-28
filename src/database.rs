use std::{convert::TryInto, sync::Arc, time::Duration};

use anyhow::Context;

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
pub struct MinutelyData {
    pub ev: String,

    #[serde(rename = "T")]
    pub t: String,

    pub e: u64,

    #[serde(flatten)]
    pub rest: serde_json::Value,
}

pub trait DatabaseHandle : Send + Sync {
    /// Get Id that is not yet added to the database, but will be the next added entry
    fn get_next_db_key(&self) -> anyhow::Result<u64>;
    fn get_first_last_id(&self) -> anyhow::Result<(Option<u64>, Option<u64>)>;
    fn get_database_disk_size(&self) -> anyhow::Result<u64>;
    fn get_entry_by_id(&self, id: u64) -> anyhow::Result<Option<MinutelyData>>;
    fn insert_entry(&self, id: u64, entry: &MinutelyData) -> anyhow::Result<()>;
    fn remove_entry(&self, id: u64) -> anyhow::Result<bool>;
}
pub type Db = std::sync::Arc<dyn DatabaseHandle>;

pub fn open_sled(db: sled::Db) -> anyhow::Result<Db> {
    Ok(Arc::new(db))
}

impl DatabaseHandle for sled::Db {
    fn get_next_db_key(&self) -> anyhow::Result<u64> {
        let nextkey = match self.last()? {
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

    fn get_first_last_id(&self) -> anyhow::Result<(Option<u64>, Option<u64>)> {
        get_first_last_id(self)
    }

    fn get_database_disk_size(&self) -> anyhow::Result<u64> {
        todo!()
    }

    fn get_entry_by_id(&self, id: u64) -> anyhow::Result<Option<MinutelyData>> {
        Ok(
            match self.get(id.to_be_bytes())? {
                Some(v) => serde_json::from_slice(&v)?,
                None => None,
            }
        )
    }

    fn insert_entry(&self, id: u64, entry: &MinutelyData) -> anyhow::Result<()> {
        self.insert(id.to_be_bytes(), serde_json::to_vec(&entry)?)?;
        Ok(())
    }

    fn remove_entry(&self, id: u64) -> anyhow::Result<bool> {
        Ok(self.remove(id.to_be_bytes())?.is_some())
    }
}

fn get_first_last_id(db : &sled::Db) -> anyhow::Result<(Option<u64>, Option<u64>)> {
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
