use crate::constants::{QUANTIZED_VECTOR_SIZE, VECTOR_SIZE};

use super::{
    metadata_index::KVPair,
    mmap_tree::{
        serialization::{TreeDeserialization, TreeSerialization},
        Tree,
    },
};
use crate::utils::quantize;
use std::hash::{Hash, Hasher};
use std::{
    fmt::Display,
    hash::DefaultHasher,
    io,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
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
    pub commit_list: Tree<u64, CommitListItem>,
    pub timestamps: Tree<u64, Vec<u64>>, // maps a timestamp to a hash
    pub commit_finish: Tree<u64, bool>,
    pub path: PathBuf,
    pub namespace_id: String,
}

impl WAL {
    pub fn new(path: PathBuf, namespace_id: String) -> io::Result<Self> {
        let commit_list_path = path.clone().join("commit_list.bin");
        let commit_list = Tree::<u64, CommitListItem>::new(commit_list_path)?;
        let timestamps_path = path.clone().join("timestamps.bin");
        let timestamps = Tree::<u64, Vec<u64>>::new(timestamps_path)?;
        let commit_finish_path = path.clone().join("commit_finish.bin");
        let commit_finish = Tree::<u64, bool>::new(commit_finish_path)?;

        Ok(WAL {
            commit_list,
            path,
            namespace_id,
            timestamps,
            commit_finish,
        })
    }

    pub fn add_to_commit_list(
        &mut self,
        hash: u64,
        vectors: Vec<[u8; QUANTIZED_VECTOR_SIZE]>,
        kvs: Vec<Vec<KVPair>>,
    ) -> Result<(), io::Error> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let commit_list_item = CommitListItem {
            hash,
            timestamp,
            vectors,
            kvs,
        };

        self.commit_list.insert(hash, commit_list_item)?;

        // self.commit_finish.insert(hash, false)?;

        // self.timestamps.insert(timestamp, hash)?;

        Ok(())
    }

    pub fn has_been_committed(&mut self, hash: u64) -> Result<bool, io::Error> {
        match self.commit_list.has_key(hash) {
            Ok(r) => Ok(r),
            Err(_) => Ok(false),
        }
    }

    // pub fn get_commits_after(&self, timestamp: u64) -> Result<Vec<CommitListItem>, io::Error> {
    //     let hashes = self.timestamps.get_range(timestamp, u64::MAX)?;

    //     let mut commits = Vec::new();

    //     for (_, hash) in hashes {
    //         match self.commit_list.search(hash) {
    //             Ok(commit) => match commit {
    //                 Some(c) => {
    //                     commits.push(c);
    //                 }
    //                 None => {}
    //             },
    //             Err(_) => {}
    //         }
    //     }

    //     Ok(commits)
    // }

    pub fn get_commits(&mut self) -> Result<Vec<CommitListItem>, io::Error> {
        let start = 0;
        let end = u64::MAX;

        let commits = self
            .commit_list
            .get_range(start, end)
            .expect("Error getting commits");

        Ok(commits.into_iter().map(|(_, v)| v).collect())
    }

    pub fn get_commit(&mut self, hash: u64) -> Result<Option<CommitListItem>, io::Error> {
        match self.commit_list.search(hash) {
            Ok(v) => Ok(v),
            Err(_) => Ok(None),
        }
    }

    pub fn get_commits_before(&mut self, timestamp: u64) -> Result<Vec<CommitListItem>, io::Error> {
        let hash_end = self.timestamps.get_range(0, timestamp)?;

        let mut commits = Vec::new();

        for (_, hash) in hash_end {
            for h in hash {
                match self.commit_list.search(h) {
                    Ok(commit) => match commit {
                        Some(c) => {
                            commits.push(c);
                        }
                        None => {}
                    },
                    Err(_) => {}
                }
            }
        }

        // println!("Commits before: {:?}", commits.len());

        Ok(commits)
    }

    pub fn get_uncommitted(&mut self, last_seconds: u64) -> Result<Vec<CommitListItem>, io::Error> {
        let start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - last_seconds;

        let end = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 1;

        let all_hashes = self.timestamps.get_range(start, end)?;

        let mut commits = Vec::new();

        for (_, hashes) in all_hashes {
            for hash in hashes {
                match self.commit_finish.has_key(hash) {
                    Ok(has_key) => {
                        if !has_key {
                            match self.commit_list.search(hash) {
                                Ok(commit) => match commit {
                                    Some(c) => {
                                        commits.push(c);
                                    }
                                    None => {}
                                },
                                Err(_) => {}
                            }
                        }
                    }
                    Err(_) => {}
                }
            }
        }

        // commits.dedup_by_key(|c| c.hash);

        Ok(commits)
    }

    pub fn compute_hash(
        &self,
        vectors: &Vec<[u8; QUANTIZED_VECTOR_SIZE]>,
        kvs: &Vec<Vec<KVPair>>,
    ) -> u64 {
        let mut hasher = DefaultHasher::default();

        // for vector in vectors {
        //     vector.hash(&mut hasher);
        // }
        vectors.hash(&mut hasher);

        kvs.hash(&mut hasher);

        hasher.finish()
    }

    pub fn add_to_wal(
        &mut self,
        vectors: Vec<[f32; VECTOR_SIZE]>,
        kvs: Vec<Vec<KVPair>>,
    ) -> io::Result<()> {
        if vectors.len() != kvs.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Quantized vectors length mismatch",
            ));
        }

        let quantized_vectors: Vec<[u8; QUANTIZED_VECTOR_SIZE]> =
            vectors.iter().map(|v| quantize(v)).collect();

        let hash = self.compute_hash(&quantized_vectors, &kvs);

        let current_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // println!("Current timestamp: {}", current_timestamp);

        let mut current_timestamp_vals = match self.timestamps.search(current_timestamp) {
            Ok(v) => v,
            Err(_) => Some(Vec::new()),
        }
        .unwrap_or(Vec::new());

        current_timestamp_vals.push(hash);

        self.timestamps
            .insert(current_timestamp, current_timestamp_vals)?;

        self.add_to_commit_list(hash, quantized_vectors, kvs)?;

        Ok(())
    }

    pub fn batch_add_to_wal(
        &mut self,
        vectors: Vec<Vec<[f32; VECTOR_SIZE]>>,
        kvs: Vec<Vec<Vec<KVPair>>>,
    ) -> io::Result<()> {
        if vectors.len() != kvs.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Quantized vectors length mismatch",
            ));
        }

        let quantized_vectors: Vec<Vec<[u8; QUANTIZED_VECTOR_SIZE]>> = vectors
            .iter()
            .map(|v| v.iter().map(|v| quantize(v)).collect())
            .collect();

        let mut hashes = Vec::new();

        let current_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut current_timestamp_vals = match self.timestamps.search(current_timestamp) {
            Ok(v) => v,
            Err(_) => Some(Vec::new()),
        }
        .unwrap_or(Vec::new());

        for (_i, (v, k)) in quantized_vectors.iter().zip(kvs.iter()).enumerate() {
            let hash = self.compute_hash(v, k);
            hashes.push(hash);

            current_timestamp_vals.push(hash);
        }

        self.timestamps
            .insert(current_timestamp, current_timestamp_vals)?;

        for (hash, (v, k)) in hashes.iter().zip(quantized_vectors.iter().zip(kvs.iter())) {
            self.add_to_commit_list(*hash, v.clone(), k.clone())?;
        }

        Ok(())
    }

    pub fn mark_commit_finished(&mut self, hash: u64) -> io::Result<()> {
        self.commit_finish.insert(hash, true)?;

        Ok(())
    }
}
