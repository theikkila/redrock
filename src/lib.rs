#[macro_use]
extern crate serde_derive;

extern crate serde;
extern crate bincode;
extern crate byteorder;

extern crate rocksdb;

use std::collections::HashMap;
use bincode::{serialize, deserialize};

use byteorder::{BigEndian, ReadBytesExt}; // 1.2.7


// extern crate time;
use std::str;
use std::mem::transmute;
// use time::PreciseTime;

use rocksdb::{DB, Options, Direction, IteratorMode, DBCompressionType, WriteBatch};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct ListMeta {
    length: u64
}


fn meta_key(tp: &str, key: &str) -> Vec<u8> {
    format!("meta_{}_{}", tp, key).as_bytes().to_vec()
}

fn data_key(tp: &str, key: &str) -> Vec<u8> {
    format!("data_{}_{}", tp, key).as_bytes().to_vec()
}

fn l_idx_key(key: &str, idx: u64) -> Vec<u8> {
    data_key("l", &format!("{}_{}", key, idx))
}
fn z_member_key(key: &str, member: &str) -> Vec<u8> {
    data_key("z", &format!("{}:{}", key, member))
}

fn z_key(key: &str) -> Vec<u8> {
    data_key("z", &format!("{}:", &key))
}

fn lmetaget(db: &rocksdb::DB, key: &str) -> ListMeta {
    let meta_k = meta_key("l", &key);
    match db.get(&meta_k) {
        Ok(Some(meta_b)) => deserialize(&meta_b).unwrap(),
        _ => ListMeta{length: 0}
    }
}

fn lmetaset(db: &rocksdb::DB, key: &str, m: &ListMeta) -> std::result::Result<(), rocksdb::Error> {
    let meta_k = meta_key("l", &key);
    db.put(&meta_k, &serialize(&m).unwrap())
}

pub fn lpush(db: &rocksdb::DB, key: &str, value: &str) -> std::result::Result<(), rocksdb::Error> {
    let mut meta = lmetaget(&db, key);
    set_str(&db, &l_idx_key(&key, meta.length), &value)
    .and_then(|_| {
        meta.length+=1;
        lmetaset(&db, &key, &meta)
    })
}

pub fn get_str(db: &rocksdb::DB, key: &[u8]) -> Option<String> {
    match db.get(key) {
        Ok(Some(data_b)) => Some(deserialize(&data_b).unwrap()),
        _ => None
    }
}
pub fn set_str(db: &rocksdb::DB, key: &[u8], data: &str) -> std::result::Result<(), rocksdb::Error> {
    db.put(&key, &serialize(&data).unwrap())
}

pub fn del(db: &rocksdb::DB, key: &[u8]) -> std::result::Result<(), rocksdb::Error> {
    db.delete(&key)
}

pub fn get_u64(db: &rocksdb::DB, key: &str) -> u64 {
    match db.get(key.as_bytes()) {
        Ok(Some(data_b)) => {
            let mut b: &[u8] = &data_b;
            b.read_u64::<BigEndian>().unwrap_or(0)
        },
        _ => 0
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

pub fn set_u64(db: &rocksdb::DB, key: &str, data: u64) -> std::result::Result<(), rocksdb::Error> {
    let bytes: [u8; 8] = unsafe { transmute(data.to_be()) };
    db.put(&key.as_bytes(), &bytes)
}

pub fn set_i64(db: &rocksdb::DB, key: &str, data: i64) -> std::result::Result<(), rocksdb::Error> {
    let bytes: [u8; 8] = unsafe { transmute(data.to_be()) };
    db.put(&key.as_bytes(), &bytes)
}

pub fn inc_u64(db: &rocksdb::DB, key: &str) -> std::result::Result<(), rocksdb::Error> {
    set_u64(&db, &key, get_u64(&db, &key)+1)
}

pub fn lget(db: &rocksdb::DB, key: &str) -> Vec<String> {
    let meta = lmetaget(&db, &key);
    let mut out: Vec<String> = vec![];
    for i in 0 .. meta.length {
        let s: String = get_str(&db, &l_idx_key(&key, i)).unwrap();
        out.push(s);
    };
    out
}

pub fn ldel(db: &rocksdb::DB, key: &str) -> std::result::Result<(), rocksdb::Error> {
    let meta = lmetaget(&db, &key);
    let mut batch = WriteBatch::default();
    for i in 0 .. meta.length {
        batch.delete(&l_idx_key(&key, i)).unwrap();
    };
    batch.delete(&meta_key("l", &key)).unwrap();
    db.write(batch)
}

pub fn lexists(db: &rocksdb::DB, key: &str) -> std::result::Result<bool, rocksdb::Error> {
    match db.get(&meta_key("l", &key)) {
        Ok(Some(_)) => Ok(true),
        Ok(None) => Ok(false),
        Err(e) => Err(e),
    }
}

pub fn sadd(db: &rocksdb::DB, key: &str, member: &str) -> std::result::Result<(), rocksdb::Error> {
    set_str(&db, &z_member_key(&key, &member), &member)
}

pub fn srem(db: &rocksdb::DB, key: &str, member: &str) -> std::result::Result<(), rocksdb::Error> {
    db.delete(&z_member_key(&key, &member))
}

pub fn prefix_search(db: &rocksdb::DB, prefix: &str) -> HashMap<String, i64> {
    let prefix_b = prefix.as_bytes();
    let mut out: HashMap<String, i64> = HashMap::new();
    let iter = db.iterator(IteratorMode::From(&prefix_b, Direction::Forward)); // From a key in Direction::{forward,reverse}
    for (it_key, it_value) in iter {
        match str::from_utf8(&it_key) {
            Ok(k) => {
                if !k.starts_with(&prefix) { break; }
                let mut b: &[u8] = &it_value;
                let v = b.read_i64::<BigEndian>().unwrap();
                out.insert(k.to_string(), v);
            },
            _ => { break; }
        }

    };
    out
}

pub fn smembers(db: &rocksdb::DB, key: &str) -> Vec<String> {
    let zk = z_key(&key);
    let zk_str = str::from_utf8(&zk).unwrap();
    let mut out: Vec<String> = vec![];
    let iter = db.iterator(IteratorMode::From(&zk, Direction::Forward)); // From a key in Direction::{forward,reverse}
    for (it_key, it_value) in iter {
        match str::from_utf8(&it_key) {
            Ok(k) => {
                if !k.starts_with(&zk_str) { break; }
                let s: String = deserialize(&it_value).unwrap();
                out.push(s);
            },
            _ => { break; }
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
    DB::open(&opts, path).unwrap()
}
