use std::{convert::TryInto, sync::{Arc, Mutex}, time::Duration};

use anyhow::Context;

use serde_derive::{Serialize, Deserialize};

const INDEX_ENTRIES : usize = 1024;

// {'T': 'b', 'S': 'TXN', 'o': 193.24, 'c': 193.24, 'h': 193.24, 'l': 193.24, 'v': 977, 't': '2021-12-13T21:04:00Z', 'n': 2, 'vw': 193.238219}
#[derive(Serialize, Deserialize)]
pub struct MinutelyData {
    #[serde(rename = "S")]
    pub ticker: String,

    #[serde(rename = "t")]
    pub start_time: String,

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
    Ok(Arc::new(SledDb{
        index: db.open_tree(b"index")?,
        root: db,
        pending_index_entries: Mutex::new(Vec::with_capacity(INDEX_ENTRIES)),
    }))
}

struct SledDb {
    root: sled::Db,
    index: sled::Tree,
    pending_index_entries: Mutex<Vec<IndexEntry>>,
}

const TRIMMED_TICKER_NAME_LEN : usize = 8;
type TrimmedTickerName = [u8; TRIMMED_TICKER_NAME_LEN];


struct IndexEntry {
    key: u64,
    nam: TrimmedTickerName,
}

#[derive(Serialize,Deserialize)]
struct IndexHeader {
    first_key: u64,
    last_key_plus_1: u64,
}

impl DatabaseHandle for SledDb {
    fn get_next_db_key(&self) -> anyhow::Result<u64> {
        let nextkey = match self.root.last()? {
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
        get_first_last_id(&self.root)
    }

    fn get_database_disk_size(&self) -> anyhow::Result<u64> {
        Ok(sled::Db::size_on_disk(&self.root)?)
    }

    fn get_entry_by_id(&self, id: u64) -> anyhow::Result<Option<MinutelyData>> {
        Ok(
            match self.root.get(id.to_be_bytes())? {
                Some(v) => serde_json::from_slice(&v)?,
                None => None,
            }
        )
    }

    fn insert_entry(&self, id: u64, entry: &MinutelyData) -> anyhow::Result<()> {
        self.root.insert(id.to_be_bytes(), serde_json::to_vec(&entry)?)?;

        let mut to_insert_to_index = None;
        let mut pe = self.pending_index_entries.lock().unwrap();
        pe.push(IndexEntry{key: id, nam: trim_ticker(&entry.ticker)});
        if pe.len() >= INDEX_ENTRIES {
            let ih = IndexHeader {
                first_key: pe[0].key,
                last_key_plus_1: pe.last().unwrap().key + 1,
            };
            let mut data = vec![0u8; TRIMMED_TICKER_NAME_LEN*(ih.last_key_plus_1 - ih.first_key) as usize];
            for IndexEntry { key, nam } in pe.drain(..) {
                if let Some(index) = key.checked_sub(ih.first_key) {
                    let index = index as usize * TRIMMED_TICKER_NAME_LEN;
                    data[index..(index+TRIMMED_TICKER_NAME_LEN)].copy_from_slice(&nam);
                }
            }
            to_insert_to_index = Some((ih, data));
        }
        drop(pe);

        if let Some((header, data)) = to_insert_to_index {
            log::debug!("Inserting to index entries {}..{}", header.first_key, header.last_key_plus_1);
            use bincode::Options;
            let header = bincode::DefaultOptions::new().with_big_endian().with_fixint_encoding().serialize(&header)?;
            self.index.insert(header, data)?;
        }

        Ok(())
    }

    fn remove_entry(&self, id: u64) -> anyhow::Result<bool> {
        Ok(self.root.remove(id.to_be_bytes())?.is_some())
    }
}

fn trim_ticker(ticker: &str) -> TrimmedTickerName {
    let mut ret = [0u8; TRIMMED_TICKER_NAME_LEN];
    let b = ticker.as_bytes();
    let cap = b.len().min(TRIMMED_TICKER_NAME_LEN);
    ret[..cap].copy_from_slice(&b[..cap]);
    ret
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
