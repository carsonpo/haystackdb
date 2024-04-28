use rayon::prelude::*;
use std::hash::Hash;
use std::path::PathBuf;
use std::{fmt::Display, sync::Mutex};

use serde::{Deserialize, Serialize};

use crate::structures::mmap_tree::Tree;

use super::mmap_tree::serialization::{TreeDeserialization, TreeSerialization};

#[derive(Debug, Serialize, Deserialize, Clone, Hash)]
pub struct KVPair {
    pub key: String,
    pub value: String,
}

impl PartialEq for KVPair {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value
    }
}

impl Eq for KVPair {}

impl PartialOrd for KVPair {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KVPair {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.value.cmp(&other.value))
    }
}

impl Display for KVPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KVPair {{ key: {}, value: {} }}", self.key, self.value)
    }
}

impl TreeSerialization for KVPair {
    fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();

        serialized.extend_from_slice(self.key.len().to_le_bytes().as_ref());
        serialized.extend_from_slice(self.key.as_bytes());
        serialized.extend_from_slice(self.value.len().to_le_bytes().as_ref());
        serialized.extend_from_slice(self.value.as_bytes());

        serialized
    }
}

impl TreeDeserialization for KVPair {
    fn deserialize(data: &[u8]) -> Self {
        let mut offset = 0;

        let key_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;
        let key = String::from_utf8(data[offset..offset + key_len].to_vec()).unwrap();
        offset += key_len;

        let value_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;
        let value = String::from_utf8(data[offset..offset + value_len].to_vec()).unwrap();
        // offset += value_len;

        KVPair { key, value }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MetadataIndexItem {
    pub kvs: Vec<KVPair>,
    pub id: u128,
    pub vector_index: usize,
    // pub namespaced_id: String,
}

impl Display for MetadataIndexItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MetadataIndexItem {{ kvs: {:?}, id: {}, vector_index: {}, namespaced_id:  }}",
            self.kvs, self.id, self.vector_index
        )
    }
}

impl TreeSerialization for MetadataIndexItem {
    fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();

        serialized.extend_from_slice(self.kvs.len().to_le_bytes().as_ref());
        // for kv in &self.kvs {
        //     serialized.extend_from_slice(kv.key.len().to_le_bytes().as_ref());
        //     serialized.extend_from_slice(kv.key.as_bytes());
        //     serialized.extend_from_slice(kv.value.len().to_le_bytes().as_ref());
        //     serialized.extend_from_slice(kv.value.as_bytes());
        // }
        for kv in &self.kvs {
            let serialized_kv = TreeSerialization::serialize(kv);
            serialized.extend_from_slice(serialized_kv.len().to_le_bytes().as_ref());
            serialized.extend_from_slice(serialized_kv.as_ref());
        }

        // serialized.extend_from_slice(self.id.len().to_le_bytes().as_ref());
        serialized.extend_from_slice(self.id.to_le_bytes().as_ref());

        serialized.extend_from_slice(self.vector_index.to_le_bytes().as_ref());

        // serialized.extend_from_slice(self.namespaced_id.len().to_le_bytes().as_ref());
        // serialized.extend_from_slice(self.namespaced_id.as_bytes());

        serialized
    }
}

impl TreeDeserialization for MetadataIndexItem {
    fn deserialize(data: &[u8]) -> Self {
        let mut offset = 0;

        let kvs_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;

        let mut kvs = Vec::new();
        for _ in 0..kvs_len {
            // let key_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
            // offset += 8;

            // let key = String::from_utf8(data[offset..offset + key_len].to_vec()).unwrap();
            // offset += key_len;

            // let value_len =
            //     u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
            // offset += 8;

            // let value = String::from_utf8(data[offset..offset + value_len].to_vec()).unwrap();
            // offset += value_len;

            // kvs.push(KVPair { key, value });

            let kv_len =
                usize::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
            offset += 8;

            let kv = TreeDeserialization::deserialize(&data[offset..offset + kv_len]);
            offset += kv_len;

            kvs.push(kv);
        }

        // let id_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
        // offset += 8;

        let id = u128::from_le_bytes(data[offset..offset + 16].try_into().unwrap());
        offset += 16;

        let vector_index = usize::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        // offset += 8;

        // let namespaced_id_len =
        //     u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
        // offset += 8;

        // let namespaced_id =
        //     String::from_utf8(data[offset..offset + namespaced_id_len].to_vec()).unwrap();
        // offset += namespaced_id_len;

        MetadataIndexItem {
            kvs,
            id,
            vector_index,
            // namespaced_id,
        }
    }
}

impl TreeSerialization for u128 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl TreeDeserialization for u128 {
    fn deserialize(data: &[u8]) -> Self {
        u128::from_le_bytes(data.try_into().unwrap())
    }
}

pub struct MetadataIndex {
    pub path: PathBuf,
    pub tree: Mutex<Tree<u128, MetadataIndexItem>>,
}

impl MetadataIndex {
    pub fn new(path: PathBuf) -> Self {
        let tree = Tree::new(path.clone()).expect("Failed to create tree");
        MetadataIndex {
            path,
            tree: Mutex::new(tree),
        }
    }

    pub fn insert(&self, key: u128, value: MetadataIndexItem) {
        // self.tree.insert(key, value).expect("Failed to insert");
        self.tree
            .lock()
            .unwrap()
            .insert(key, value)
            .expect("Failed to insert");
    }

    pub fn batch_insert(&mut self, items: Vec<(u128, MetadataIndexItem)>) {
        items
            .par_iter()
            .enumerate()
            .for_each(|(idx, (key, value))| {
                // println!("Inserting id {} of {}", idx, items.len());
                self.insert(*key, value.clone());
                println!("Inserted id {} of {}", idx, items.len());
            });
    }

    pub fn get(&mut self, key: u128) -> Option<MetadataIndexItem> {
        match self.tree.lock().unwrap().search(key) {
            Ok(v) => v,
            Err(_) => None,
        }
    }
}
