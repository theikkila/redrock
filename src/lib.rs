#[macro_use]
extern crate serde_derive;

extern crate serde;
extern crate bincode;
extern crate byteorder;

extern crate rocksdb;

use std::collections::HashMap;
use bincode::{serialize, deserialize};

use byteorder::{BigEndian, ReadBytesExt}; // 1.2.7


use std::error;
use std::fmt;
use std::str;

use rocksdb::{DB, Options, Direction, IteratorMode, DBCompressionType, WriteBatch};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct ListMeta {
    length: u64
}

#[derive(Debug, Clone, PartialEq)]
pub struct RedrockError {
    message: String,
}

impl From<RedrockError> for String {
    fn from(e: RedrockError) -> String {
        e.message
    }
}

impl From<rocksdb::Error> for RedrockError {
    fn from(e: rocksdb::Error) -> RedrockError {
        RedrockError{message: e.into_string()}
    }
}
impl From<bincode::Error> for RedrockError {
    fn from(e: bincode::Error) -> RedrockError {
        RedrockError{message: format!("{:?}", e)}
    }
}

impl error::Error for RedrockError {
    fn description(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for RedrockError {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.message.fmt(formatter)
    }
}

// impl fmt::Debug for Point {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "RedRockError: {}", self)
//     }
// }


fn meta_key(tp: &str, key: &str) -> String {
    format!("meta_{}_{}", tp, key)//.as_bytes().to_vec()
}

fn data_key(tp: &str, key: &str) -> String {
    format!("data_{}_{}", tp, key)//.as_bytes().to_vec()
}

fn l_idx_key(key: &str, idx: u64) -> String {
    data_key("l", &format!("{}_{}", key, idx))
}
fn z_member_key(key: &str, member: &str) -> String {
    data_key("z", &format!("{}:{}", key, member))
}

fn z_key(key: &str) -> String {
    data_key("z", &format!("{}:", &key))
}

fn lmetaget(db: &rocksdb::DB, key: &str) -> Result<ListMeta, RedrockError> {
    let meta_k = meta_key("l", &key);
    match db.get(&meta_k) {
        Ok(Some(meta_b)) => deserialize(&meta_b).map_err(|e| e.into()),
        _ => Ok(ListMeta{length: 0})
    }
}

fn lmetaset(db: &rocksdb::DB, key: &str, m: &ListMeta) -> std::result::Result<(), RedrockError> {
    let meta_k = meta_key("l", &key);
    db.put(&meta_k, &serialize(&m)?).map_err(|e| e.into())
}

pub fn lpush(db: &rocksdb::DB, key: &str, value: &str) -> std::result::Result<(), RedrockError> {
    let mut meta = lmetaget(&db, key)?;
    set_str(&db, &l_idx_key(&key, meta.length), &value)
    .and_then(|_| {
        meta.length+=1;
        lmetaset(&db, &key, &meta)
    })
}

pub fn llen(db: &rocksdb::DB, key: &str) -> std::result::Result<u64, RedrockError> {
    let meta = lmetaget(&db, &key)?;
    Ok(meta.length)
}

pub fn get_str(db: &rocksdb::DB, key: &str) -> Option<String> {
    match db.get(key) {
        Ok(Some(data_b)) => deserialize(&data_b).ok(),
        _ => None
    }
}
pub fn set_str(db: &rocksdb::DB, key: &str, data: &str) -> std::result::Result<(), RedrockError> {
    db.put(&key.as_bytes(), &serialize(&data)?).map_err(|e| e.into())
}

pub fn del(db: &rocksdb::DB, key: &str) -> std::result::Result<(), RedrockError> {
    db.delete(&key.as_bytes()).map_err(|e| e.into())
}

pub fn get_u64(db: &rocksdb::DB, key: &str) -> Option<u64> {
    match db.get(key.as_bytes()) {
        Ok(Some(data_b)) => {
            let mut b: &[u8] = &data_b;
            b.read_u64::<BigEndian>().ok()
        },
        _ => None
    }
}

pub fn get_i64(db: &rocksdb::DB, key: &str) -> Option<i64> {
    match db.get(key.as_bytes()) {
        Ok(Some(data_b)) => {
            let mut b: &[u8] = &data_b;
            b.read_i64::<BigEndian>().ok()
        },
        _ => None
    }
}

pub fn set_u64(db: &rocksdb::DB, key: &str, data: u64) -> std::result::Result<(), RedrockError> {
    let bytes: [u8; 8] = data.to_be_bytes();
    db.put(&key.as_bytes(), &bytes).map_err(|e| e.into())
}

pub fn set_i64(db: &rocksdb::DB, key: &str, data: i64) -> std::result::Result<(), RedrockError> {
    let bytes: [u8; 8] = data.to_be_bytes();
    db.put(&key.as_bytes(), &bytes).map_err(|e| e.into())
}

pub fn inc_u64(db: &rocksdb::DB, key: &str) -> std::result::Result<(), RedrockError> {
    set_u64(&db, &key, get_u64(&db, &key).unwrap_or(0)+1)
}

pub fn inc_i64(db: &rocksdb::DB, key: &str) -> std::result::Result<(), RedrockError> {
    set_i64(&db, &key, get_i64(&db, &key).unwrap_or(0)+1)
}

pub fn lget(db: &rocksdb::DB, key: &str) -> Vec<String> {
    let mut out: Vec<String> = vec![];
    if let Ok(meta) = lmetaget(&db, &key) {
        for i in 0 .. meta.length {
            get_str(&db, &l_idx_key(&key, i))
            .map(|s| out.push(s));
        };
    }
    out
}

pub fn ldel(db: &rocksdb::DB, key: &str) -> std::result::Result<(), RedrockError> {
    let meta = lmetaget(&db, &key)?;
    let mut batch = WriteBatch::default();
    for i in 0 .. meta.length {
        batch.delete(&l_idx_key(&key, i));
    };
    batch.delete(&meta_key("l", &key));
    db.write(batch).map_err(|e| e.into())
}

pub fn lexists(db: &rocksdb::DB, key: &str) -> std::result::Result<bool, RedrockError> {
    match db.get(&meta_key("l", &key)) {
        Ok(Some(_)) => Ok(true),
        Ok(None) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

pub fn sadd(db: &rocksdb::DB, key: &str, member: &str) -> std::result::Result<(), RedrockError> {
    set_str(&db, &z_member_key(&key, &member), &member)
}

pub fn srem(db: &rocksdb::DB, key: &str, member: &str) -> std::result::Result<(), RedrockError> {
    db.delete(&z_member_key(&key, &member)).map_err(|e| e.into())
}

pub fn prefix_search(db: &rocksdb::DB, prefix: &str) -> HashMap<String, i64> {
    let prefix_b = prefix.as_bytes();
    let mut out: HashMap<String, i64> = HashMap::new();
    let iter = db.iterator(IteratorMode::From(&prefix_b, Direction::Forward)); // From a key in Direction::{forward,reverse}
    for item in iter {
        if let Ok(kv) = item {
            let (it_key, it_value) = kv;
            match str::from_utf8(&it_key) {
                Ok(k) => {
                    if !k.starts_with(&prefix) { break; }
                    let mut b: &[u8] = &it_value;
                    b.read_i64::<BigEndian>()
                    .map(|v| out.insert(k.to_string(), v)).expect("inserting to hashmap");
                },
                _ => { break; }
            }
        }
    };
    out
}
pub fn prefix_search_str(db: &rocksdb::DB, prefix: &str) -> HashMap<String, String> {
    let prefix_b = prefix.as_bytes();
    let mut out: HashMap<String, String> = HashMap::new();
    let iter = db.iterator(IteratorMode::From(&prefix_b, Direction::Forward));
    for item in iter {
        if let Ok(kv) = item {
            let (it_key, it_value) = kv;

            match str::from_utf8(&it_key) {
                Ok(k) => {
                    if !k.starts_with(&prefix) { break; }
                    deserialize(&it_value).map(|s| out.insert(k.to_string(), s)).expect("inserting to hashmap");
                },
                _ => { break; }
            }
        }
    };
    out
}

pub fn smembers(db: &rocksdb::DB, key: &str) -> Vec<String> {
    let zk = z_key(&key);
    let mut out: Vec<String> = vec![];
    let iter = db.iterator(IteratorMode::From(&zk.as_bytes(), Direction::Forward)); // From a key in Direction::{forward,reverse}
    for item in iter {
        if let Ok(kv) = item {
            let (it_key, it_value) = kv;
            match str::from_utf8(&it_key) {
                Ok(k) => {
                    if !k.starts_with(&zk) { break; }
                    deserialize(&it_value).map(|s| out.push(s)).expect("inserting to vector");
                },
                _ => { break; }
            }
        }
    };
    out
}

pub fn open_db(path: &str) -> rocksdb::DB {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(DBCompressionType::Lz4);
    // let transform = SliceTransform::create_fixed_prefix(5);
    // opts.increase_parallelism(4);
    // opts.enable_statistics();
    // opts.set_stats_dump_period_sec(120);
    // opts.set_prefix_extractor(transform);
    // opts.set_memtable_prefix_bloom_ratio(0.1);
    DB::open(&opts, path).expect("open database")
}
