use std::{convert::TryInto, sync::{Arc, Mutex}, time::Duration, iter::FromIterator};

use anyhow::Context;

use bincode::Options;
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
    fn get_filtered_reader(&self, filter: &hashbrown::HashSet<smol_str::SmolStr>) -> Option<Box<dyn Send + FilteredDatabaseReader>>;
}
pub type Db = std::sync::Arc<dyn DatabaseHandle>;

pub trait FilteredDatabaseReader {
    fn get_entry_by_id_if_matches(&mut self, id: u64) -> anyhow::Result<Option<MinutelyData>>;
}

const INDEX_TREE_NAME : &'static [u8] = b"index";
pub fn open_sled(db: sled::Db) -> anyhow::Result<Db> {
    Ok(Arc::new(SledDb{
        index: db.open_tree(INDEX_TREE_NAME)?,
        root: db,
        pending_index_entries: Mutex::new(Vec::with_capacity(INDEX_ENTRIES)),
    }))
}

struct SledDb {
    root: sled::Db,
    index: sled::Tree,
    pending_index_entries: Mutex<Vec<IndexEntry>>,
}

struct SledFilteredReader {
    filter: hashbrown::HashSet<TrimmedTickerName>,
    root: sled::Db,
    index: sled::Tree,
    current_filter_entry : Option<(IndexHeader, hashbrown::HashMap<u64, TrimmedTickerName>)>,
    index_is_stale_until: Option<u64>,
}

const TRIMMED_TICKER_NAME_LEN : usize = 4;
type TrimmedTickerName = [u8; TRIMMED_TICKER_NAME_LEN];


struct IndexEntry {
    key: u64,
    nam: TrimmedTickerName,
}

#[derive(Serialize,Deserialize,Clone, Copy)]
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
            let header = bco().serialize(&header)?;
            self.index.insert(header, data)?;
        }

        Ok(())
    }

    fn remove_entry(&self, id: u64) -> anyhow::Result<bool> {
        Ok(self.root.remove(id.to_be_bytes())?.is_some())
    }

    fn get_filtered_reader(&self, filter: &hashbrown::HashSet<smol_str::SmolStr>) -> Option<Box<dyn Send + FilteredDatabaseReader>> {
        Some(Box::new(SledFilteredReader {
            filter : hashbrown::HashSet::from_iter(filter.iter().map(|x|trim_ticker(x.as_str()))),
            current_filter_entry: None,
            root: self.root.clone(),
            index: self.index.clone(),
            index_is_stale_until: None,
        }))
    }
}

fn bco() -> impl bincode::Options {
    bincode::DefaultOptions::new().with_big_endian().with_fixint_encoding()
}

impl FilteredDatabaseReader for SledFilteredReader {
    fn get_entry_by_id_if_matches(&mut self, id: u64) -> anyhow::Result<Option<MinutelyData>> {
        if let Some(ref e) = self.current_filter_entry {
            if e.0.first_key <= id && e.0.last_key_plus_1 > id {
                // OK, this is a valid entry
                log::trace!("Index page is already OK");
            } else {
                log::trace!("Index page gets invalidated");
                self.current_filter_entry = None;
            }
        }
        if let Some(isu) = self.index_is_stale_until {
            if id > isu {
                self.index_is_stale_until = None;
                log::debug!("Resetting index stale status");
            }
        }
        if self.current_filter_entry.is_none() && self.index_is_stale_until.is_none() {
            let ih: IndexHeader = IndexHeader {
                first_key: id,
                last_key_plus_1: u64::MAX,
            };
            let ih = bco().serialize(&ih)?;
            if let Some((ie_h, ie_data)) = self.index.get_lt(ih)? {
                let ih : IndexHeader = bco().deserialize(&ie_h.to_vec())?;
                if ih.first_key <= id && ih.last_key_plus_1 > id {
                    // OK, this is a valid entry
                    let mut map : hashbrown::HashMap<u64, TrimmedTickerName> = hashbrown::HashMap::with_capacity(INDEX_ENTRIES);
                    for (i,chunk) in ie_data.chunks_exact(TRIMMED_TICKER_NAME_LEN).enumerate() {
                        let nam : [u8; TRIMMED_TICKER_NAME_LEN] = chunk.try_into().unwrap();
                        map.insert(ih.first_key + i as u64, nam);
                    }
                    self.current_filter_entry=Some((ih, map));
                    log::debug!("Loaded index page {}..{}", ih.first_key, ih.last_key_plus_1);
                } else {
                    log::debug!("Index is marked as stale - wrong id. id={} not in {}..{}", id, ih.first_key, ih.last_key_plus_1);
                    self.index_is_stale_until = Some(id + INDEX_ENTRIES as u64 / 2);
                }
            } else {
                log::debug!("Index is marked as stale - not found for id={}", id);
                self.index_is_stale_until = Some(id + INDEX_ENTRIES as u64 / 2);
            }
        }
        if let Some(ref cfe) = self.current_filter_entry {
            if ! self.filter.contains(&cfe.1[&id]) {
                log::trace!("{}({}) does not match filter", id, String::from_utf8_lossy(&cfe.1[&id]));
                return Ok(None);
            } else {
                log::trace!("{}({}) does matches the filter", id, String::from_utf8_lossy(&cfe.1[&id]));
            }
        } 
        // unconditionally let the item though.
        // The code in `client.rs` should provide additional filtering anyway
        Ok(
            match self.root.get(id.to_be_bytes())? {
                Some(v) => serde_json::from_slice(&v)?,
                None => None,
            }
        )
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

            // Now clear stale index entries
            match db.first()? {
                None => (),
                Some((k, _v)) => {
                    let k: [u8; 8] = k.as_ref().try_into()?;
                    let first_remaining_key = u64::from_be_bytes(k);
                    let index = db.open_tree(INDEX_TREE_NAME)?;
                    for index_entry in index.iter() {
                        let (index_key, _) = index_entry?;
                        let range : IndexHeader = bco().deserialize(&index_key.to_vec())?;
                        if range.last_key_plus_1 <= first_remaining_key {
                            index.remove(index_key)?;
                        } else {
                            break;
                        }
                    }
                }
            };

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
