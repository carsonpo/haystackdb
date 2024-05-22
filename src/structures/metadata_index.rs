use std::fmt::Display;
use std::hash::Hash;
use std::hash::Hasher;

use serde::{Deserialize, Serialize};

use crate::structures::tree::Tree;

use super::tree::serialization::{TreeDeserialization, TreeSerialization};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum KVValue {
    String(String),
    Integer(i64),
    Float(f32),
}

impl Display for KVValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KVValue::String(s) => write!(f, "{}", s),
            KVValue::Integer(i) => write!(f, "{}", i),
            KVValue::Float(fl) => write!(f, "{}", fl),
        }
    }
}

impl Hash for KVValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            KVValue::String(s) => s.hash(state),
            KVValue::Integer(i) => i.hash(state),
            KVValue::Float(f) => {
                let bits: u32 = f.to_bits();
                bits.hash(state);
            }
        }
    }
}

impl PartialEq for KVValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (KVValue::String(s1), KVValue::String(s2)) => s1 == s2,
            (KVValue::Integer(i1), KVValue::Integer(i2)) => i1 == i2,
            (KVValue::Float(f1), KVValue::Float(f2)) => (f1 - f2).abs() < 1e-6,
            _ => false,
        }
    }
}

impl Eq for KVValue {}

impl PartialOrd for KVValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KVValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (KVValue::String(s1), KVValue::String(s2)) => s1.cmp(s2),
            (KVValue::Integer(i1), KVValue::Integer(i2)) => i1.cmp(i2),
            (KVValue::Float(f1), KVValue::Float(f2)) => f1.partial_cmp(f2).unwrap(),
            _ => std::cmp::Ordering::Less,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash)]
pub struct KVPair {
    pub key: String,
    pub value: KVValue,
}

impl KVPair {
    pub fn new(key: String, value: String) -> Self {
        KVPair {
            key,
            value: KVValue::String(value),
        }
    }

    pub fn new_int(key: String, value: i64) -> Self {
        KVPair {
            key,
            value: KVValue::Integer(value),
        }
    }

    pub fn new_float(key: String, value: f32) -> Self {
        KVPair {
            key,
            value: KVValue::Float(value),
        }
    }
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
        // serialized.extend_from_slice(self.value.len().to_le_bytes().as_ref());
        // serialized.extend_from_slice(self.value.as_bytes());

        match self.value.clone() {
            KVValue::String(s) => {
                serialized.push(0);
                serialized.extend_from_slice(s.len().to_le_bytes().as_ref());
                serialized.extend_from_slice(s.as_bytes());
            }
            KVValue::Integer(i) => {
                serialized.push(1);
                serialized.extend_from_slice(i.to_le_bytes().as_ref());
            }
            KVValue::Float(f) => {
                serialized.push(2);
                serialized.extend_from_slice(f.to_bits().to_le_bytes().as_ref());
            }
        }

        serialized
    }
}

impl KVPair {
    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();

        serialized.extend_from_slice(self.key.len().to_le_bytes().as_ref());
        serialized.extend_from_slice(self.key.as_bytes());
        // serialized.extend_from_slice(self.value.len().to_le_bytes().as_ref());
        // serialized.extend_from_slice(self.value.as_bytes());

        match self.value.clone() {
            KVValue::String(s) => {
                serialized.push(0);
                serialized.extend_from_slice(s.len().to_le_bytes().as_ref());
                serialized.extend_from_slice(s.as_bytes());
            }
            KVValue::Integer(i) => {
                serialized.push(1);
                serialized.extend_from_slice(i.to_le_bytes().as_ref());
            }
            KVValue::Float(f) => {
                serialized.push(2);
                serialized.extend_from_slice(f.to_bits().to_le_bytes().as_ref());
            }
        }

        serialized
    }

    pub fn deserialize(data: &[u8]) -> Self {
        let mut offset = 0;

        let key_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;
        let key = String::from_utf8(data[offset..offset + key_len].to_vec()).unwrap();
        offset += key_len;

        // let value_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
        // offset += 8;
        // let value = String::from_utf8(data[offset..offset + value_len].to_vec()).unwrap();
        // // offset += value_len;

        let value_flag = data[offset];
        offset += 1;

        let value = match value_flag {
            0 => {
                let value_len =
                    u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
                offset += 8;
                let value = String::from_utf8(data[offset..offset + value_len].to_vec()).unwrap();
                KVValue::String(value)
            }
            1 => {
                let value = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                KVValue::Integer(value)
            }
            2 => {
                let bits = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                let value = f32::from_bits(bits);
                KVValue::Float(value)
            }
            _ => KVValue::String("".to_string()),
        };

        KVPair { key, value }
    }
}

impl TreeDeserialization for KVPair {
    fn deserialize(data: &[u8]) -> Self {
        let mut offset = 0;

        let key_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;
        let key = String::from_utf8(data[offset..offset + key_len].to_vec()).unwrap();
        offset += key_len;

        // let value_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
        // offset += 8;
        // let value = String::from_utf8(data[offset..offset + value_len].to_vec()).unwrap();
        // // offset += value_len;

        let value_flag = data[offset];
        offset += 1;

        let value = match value_flag {
            0 => {
                let value_len =
                    u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
                offset += 8;
                let value = String::from_utf8(data[offset..offset + value_len].to_vec()).unwrap();
                KVValue::String(value)
            }
            1 => {
                let value = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                KVValue::Integer(value)
            }
            2 => {
                let bits = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                let value = f32::from_bits(bits);
                KVValue::Float(value)
            }
            _ => KVValue::String("".to_string()),
        };

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

            let kv_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
            offset += 8;

            let kv = TreeDeserialization::deserialize(&data[offset..offset + kv_len]);
            offset += kv_len;

            kvs.push(kv);
        }

        // let id_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
        // offset += 8;

        let id = u128::from_le_bytes(data[offset..offset + 16].try_into().unwrap());
        offset += 16;

        let vector_index =
            u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
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
    pub tree: Tree<u128, MetadataIndexItem>,
}

impl MetadataIndex {
    pub fn new() -> Self {
        let tree = Tree::new().expect("Failed to create tree");
        MetadataIndex { tree }
    }

    pub fn insert(&mut self, key: u128, value: MetadataIndexItem) {
        // self.tree.insert(key, value).expect("Failed to insert");
        self.tree.insert(key, value).expect("Failed to insert");
    }

    pub fn batch_insert(&mut self, items: Vec<(u128, MetadataIndexItem)>) {
        self.tree
            .batch_insert(items)
            .expect("Failed to batch insert");
    }

    pub fn get(&mut self, key: u128) -> Option<MetadataIndexItem> {
        match self.tree.search(key) {
            Ok(v) => v,
            Err(_) => None,
        }
    }

    pub fn len(&self) -> usize {
        self.tree.len()
    }

    pub fn to_binary(&mut self) -> Vec<u8> {
        self.tree.to_binary()
    }

    pub fn from_binary(data: Vec<u8>) -> Self {
        let tree = Tree::from_binary(data).expect("Failed to create tree from binary");
        MetadataIndex { tree }
    }
}
