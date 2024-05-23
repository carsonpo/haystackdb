use crate::constants::{QUANTIZED_VECTOR_SIZE, VECTOR_SIZE};

use super::mmap_tree::{
    serialization::{TreeDeserialization, TreeSerialization},
    Tree,
};
use crate::structures::filters::KVPair;
use crate::utils::quantize;
use chrono::{NaiveDateTime, Utc};
use rusqlite::{params, Connection, Error, Result, ToSql};
use serde_json::json;
use std::{
    fmt::Display,
    fs,
    hash::DefaultHasher,
    io,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

#[derive(Debug, Clone)]
pub struct CommitListItem {
    pub hash: u64,
    pub timestamp: u64,
    pub vectors: Vec<[u8; QUANTIZED_VECTOR_SIZE]>,
    pub kvs: Vec<Vec<KVPair>>,
}

impl Display for CommitListItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CommitListItem {{ hash: {}, timestamp: {}}}",
            self.hash, self.timestamp
        )
    }
}

impl TreeSerialization for CommitListItem {
    fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();

        serialized.extend_from_slice(self.hash.to_le_bytes().as_ref());
        serialized.extend_from_slice(self.timestamp.to_le_bytes().as_ref());

        serialized.extend_from_slice(self.vectors.len().to_le_bytes().as_ref());
        for vector in &self.vectors {
            serialized.extend_from_slice(vector.as_ref());
        }

        serialized.extend_from_slice(self.kvs.len().to_le_bytes().as_ref());
        for sub_kvs in &self.kvs {
            serialized.extend_from_slice(sub_kvs.len().to_le_bytes().as_ref());
            for kv in sub_kvs {
                serialized.extend_from_slice(&kv.serialize());
            }
        }

        serialized
    }
}

impl TreeDeserialization for CommitListItem {
    fn deserialize(data: &[u8]) -> Self {
        let mut offset = 0;

        let hash = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let timestamp = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let vectors_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;

        offset += 8;

        let mut vectors = Vec::new();
        for _ in 0..vectors_len {
            let mut vector = [0; QUANTIZED_VECTOR_SIZE];
            vector.copy_from_slice(&data[offset..offset + QUANTIZED_VECTOR_SIZE]);
            offset += QUANTIZED_VECTOR_SIZE;
            vectors.push(vector);
        }

        let kvs_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;

        let mut kvs = Vec::new();
        for _ in 0..kvs_len {
            let mut sub_kvs = Vec::new();
            let sub_kvs_len =
                u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
            offset += 8;
            for _ in 0..sub_kvs_len {
                let kv = KVPair::deserialize(&data[offset..]);
                offset += kv.serialize().len();
                sub_kvs.push(kv);
            }

            kvs.push(sub_kvs);
        }

        CommitListItem {
            hash,
            timestamp,
            kvs,
            vectors,
        }
    }
}

impl TreeSerialization for bool {
    fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();

        serialized.extend_from_slice(&[*self as u8]);

        serialized
    }
}

impl TreeDeserialization for bool {
    fn deserialize(data: &[u8]) -> Self {
        data[0] == 1
    }
}

impl TreeSerialization for u64 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl TreeDeserialization for u64 {
    fn deserialize(data: &[u8]) -> Self {
        u64::from_le_bytes(data.try_into().unwrap())
    }
}

impl TreeSerialization for Vec<u64> {
    fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();

        serialized.extend_from_slice(self.len().to_le_bytes().as_ref());
        for val in self {
            serialized.extend_from_slice(val.to_le_bytes().as_ref());
        }

        serialized
    }
}

impl TreeDeserialization for Vec<u64> {
    fn deserialize(data: &[u8]) -> Self {
        let mut offset = 0;

        let len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;

        let mut vals = Vec::new();
        for _ in 0..len {
            let val = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;
            vals.push(val);
        }

        vals
    }
}

pub struct WAL {
    pub conn: Connection,
    pub path: PathBuf,
    pub namespace_id: String,
}

impl WAL {
    pub fn new(path: PathBuf, namespace_id: String) -> Result<Self> {
        let db_path = path.join("wal.db");

        // Create the directory if it doesn't exist
        // fs::create_dir_all(&path).expect("Failed to create directory");

        let conn = Connection::open(db_path.clone())?;

        // Enable WAL mode
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 30000;")?;

        // Create the table if it doesn't exist
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS wal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash INTEGER NOT NULL,
                data BLOB NOT NULL,
                metadata TEXT NOT NULL,
                added_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                committed_timestamp DATETIME
            );",
        )?;

        Ok(WAL {
            conn,
            path: db_path,
            namespace_id,
        })
    }

    fn u64_to_i64(&self, value: u64) -> i64 {
        // Safely convert u64 to i64 by reinterpreting the bits
        i64::from_ne_bytes(value.to_ne_bytes())
    }

    fn i64_to_u64(&self, value: i64) -> u64 {
        // Safely convert i64 to u64 by reinterpreting the bits
        u64::from_ne_bytes(value.to_ne_bytes())
    }

    pub fn add_to_commit_list(
        &mut self,
        hash: u64,
        vectors: Vec<[u8; QUANTIZED_VECTOR_SIZE]>,
        kvs: Vec<Vec<KVPair>>,
    ) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let metadata = json!(kvs).to_string();
        let data: Vec<u8> = vectors.iter().flat_map(|v| v.to_vec()).collect();

        self.conn.execute(
            "INSERT INTO wal (hash, data, metadata, added_timestamp) VALUES (?1, ?2, ?3, ?4);",
            params![
                self.u64_to_i64(hash),
                &data,
                &metadata,
                self.u64_to_i64(timestamp)
            ],
        )?;

        Ok(())
    }

    pub fn has_been_committed(&mut self, hash: u64) -> Result<bool> {
        let mut stmt = self
            .conn
            .prepare("SELECT 1 FROM wal WHERE hash = ?1 AND committed_timestamp IS NOT NULL;")?;
        let mut rows = stmt.query(params![hash])?;
        Ok(rows.next()?.is_some())
    }

    pub fn get_commits(&mut self) -> Result<Vec<CommitListItem>> {
        let mut stmt = self.conn.prepare(
            "SELECT hash, data, metadata, added_timestamp, committed_timestamp FROM wal;",
        )?;
        let rows = stmt.query_map(params![], |row| {
            let data: Vec<u8> = row.get(1)?;
            let vectors = data
                .chunks(QUANTIZED_VECTOR_SIZE)
                .map(|chunk| {
                    let mut arr = [0; QUANTIZED_VECTOR_SIZE];
                    arr.copy_from_slice(chunk);
                    arr
                })
                .collect();

            Ok(CommitListItem {
                hash: self.i64_to_u64(row.get(0)?),
                timestamp: self.i64_to_u64(row.get(3)?),
                vectors,
                kvs: serde_json::from_str(&row.get::<_, String>(2)?).unwrap(),
            })
        })?;

        let mut commits = Vec::new();
        for commit in rows {
            commits.push(commit?);
        }

        Ok(commits)
    }

    pub fn get_commit(&mut self, hash: u64) -> Result<Option<CommitListItem>> {
        let mut stmt = self.conn.prepare("SELECT hash, data, metadata, added_timestamp, committed_timestamp FROM wal WHERE hash = ?1;")?;
        let mut rows = stmt.query(params![hash])?;

        if let Some(row) = rows.next()? {
            let data: Vec<u8> = row.get(1)?;
            let vectors = data
                .chunks(QUANTIZED_VECTOR_SIZE)
                .map(|chunk| {
                    let mut arr = [0; QUANTIZED_VECTOR_SIZE];
                    arr.copy_from_slice(chunk);
                    arr
                })
                .collect();

            return Ok(Some(CommitListItem {
                hash: self.i64_to_u64(row.get(0)?),
                timestamp: self.i64_to_u64(row.get(3)?),
                vectors,
                kvs: serde_json::from_str(&row.get::<_, String>(2)?).unwrap(),
            }));
        }

        Ok(None)
    }

    pub fn mark_commit_finished(&mut self, hash: u64) -> Result<()> {
        let committed_timestamp = Utc::now().naive_utc();
        self.conn.execute(
            "UPDATE wal SET committed_timestamp = ?1 WHERE hash = ?2;",
            params![committed_timestamp.to_string(), self.u64_to_i64(hash)],
        )?;

        Ok(())
    }

    pub fn compute_hash(
        &self,
        vectors: &Vec<[u8; QUANTIZED_VECTOR_SIZE]>,
        kvs: &Vec<Vec<KVPair>>,
    ) -> u64 {
        let mut hasher = DefaultHasher::new();
        vectors.hash(&mut hasher);
        kvs.hash(&mut hasher);
        hasher.finish()
    }

    pub fn add_to_wal(
        &mut self,
        vectors: Vec<[f32; VECTOR_SIZE]>,
        kvs: Vec<Vec<KVPair>>,
    ) -> Result<()> {
        let quantized_vectors: Vec<[u8; QUANTIZED_VECTOR_SIZE]> =
            vectors.iter().map(|v| quantize(v)).collect();
        let hash = self.compute_hash(&quantized_vectors, &kvs);
        self.add_to_commit_list(hash, quantized_vectors, kvs)
    }

    pub fn batch_add_to_wal(
        &mut self,
        vectors: Vec<Vec<[f32; VECTOR_SIZE]>>,
        kvs: Vec<Vec<Vec<KVPair>>>,
    ) -> Result<()> {
        let quantized_vectors: Vec<Vec<[u8; QUANTIZED_VECTOR_SIZE]>> = vectors
            .iter()
            .map(|v| v.iter().map(|v| quantize(v)).collect())
            .collect();

        for (v, k) in quantized_vectors.iter().zip(kvs.iter()) {
            let hash = self.compute_hash(v, k);
            self.add_to_commit_list(hash, v.clone(), k.clone())?;
        }

        Ok(())
    }

    pub fn get_uncommitted(&mut self, last_seconds: u64) -> Result<Vec<CommitListItem>> {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let start_time = self.u64_to_i64(current_time.saturating_sub(last_seconds));

        let mut stmt = self.conn.prepare(
            "SELECT hash, data, metadata, added_timestamp, committed_timestamp
             FROM wal
             WHERE added_timestamp >= ?1 AND committed_timestamp IS NULL;",
        )?;
        let rows = stmt.query_map(params![start_time], |row| {
            let data: Vec<u8> = row.get(1)?;
            let vectors = data
                .chunks(QUANTIZED_VECTOR_SIZE)
                .map(|chunk| {
                    let mut arr = [0; QUANTIZED_VECTOR_SIZE];
                    arr.copy_from_slice(chunk);
                    arr
                })
                .collect();

            Ok(CommitListItem {
                hash: self.i64_to_u64(row.get(0)?),
                timestamp: self.i64_to_u64(row.get(3)?),
                vectors,
                kvs: serde_json::from_str(&row.get::<_, String>(2)?).unwrap(),
            })
        })?;

        let mut commits = Vec::new();
        for commit in rows {
            commits.push(commit?);
        }

        Ok(commits)
    }

    pub fn get_commits_before(&self, ts: u64) -> Result<Vec<CommitListItem>> {
        let mut stmt = self.conn.prepare(
            "SELECT hash, data, metadata, added_timestamp, committed_timestamp
             FROM wal
             WHERE added_timestamp < ?1 AND committed_timestamp IS NOT NULL;",
        )?;
        let rows = stmt.query_map(params![self.u64_to_i64(ts)], |row| {
            let data: Vec<u8> = row.get(1)?;
            let vectors = data
                .chunks(QUANTIZED_VECTOR_SIZE)
                .map(|chunk| {
                    let mut arr = [0; QUANTIZED_VECTOR_SIZE];
                    arr.copy_from_slice(chunk);
                    arr
                })
                .collect();

            Ok(CommitListItem {
                hash: self.i64_to_u64(row.get(0)?),
                timestamp: self.i64_to_u64(row.get(3)?),
                vectors,
                kvs: serde_json::from_str(&row.get::<_, String>(2)?).unwrap(),
            })
        })?;

        let mut commits = Vec::new();
        for commit in rows {
            commits.push(commit?);
        }

        Ok(commits)
    }
}
