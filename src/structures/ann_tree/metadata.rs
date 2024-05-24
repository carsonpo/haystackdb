use std::path::PathBuf;

// use std::collections::HashSet;
use ahash::{AHashMap as HashMap, AHashSet as HashSet};

use serde::{Deserialize, Serialize};

use crate::structures::filters::Filter;
// use crate::structures::metadata_index::{KVPair, KVValue};
use crate::structures::filters::{KVPair, KVValue};
use crate::structures::mmap_tree::Tree;
use std::io;

use crate::structures::mmap_tree::serialization::{TreeDeserialization, TreeSerialization};

#[derive(Debug, Clone)]
pub struct NodeMetadata {
    pub values: HashSet<String>,
    pub int_range: Option<(i64, i64)>,
    pub float_range: Option<(f32, f32)>,
}

impl NodeMetadata {
    pub fn new() -> Self {
        NodeMetadata {
            values: HashSet::new(),
            int_range: None,
            float_range: None,
        }
    }
}

impl TreeSerialization for NodeMetadata {
    fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();

        serialized.extend_from_slice(self.values.len().to_le_bytes().as_ref());
        for value in &self.values {
            serialized.extend_from_slice(value.len().to_le_bytes().as_ref());
            serialized.extend_from_slice(value.as_bytes());
        }

        if let Some((start, end)) = self.int_range {
            serialized.extend_from_slice(start.to_le_bytes().as_ref());
            serialized.extend_from_slice(end.to_le_bytes().as_ref());
        }

        if let Some((start, end)) = self.float_range {
            serialized.extend_from_slice(start.to_le_bytes().as_ref());
            serialized.extend_from_slice(end.to_le_bytes().as_ref());
        }

        serialized
    }
}

impl TreeDeserialization for NodeMetadata {
    fn deserialize(serialized: &[u8]) -> Self {
        let mut values = HashSet::new();

        let mut offset = 0;

        let values_len =
            u64::from_le_bytes(serialized[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;

        for _ in 0..values_len {
            let value_len =
                u64::from_le_bytes(serialized[offset..offset + 8].try_into().unwrap()) as usize;
            offset += 8;

            let value = String::from_utf8(serialized[offset..offset + value_len].to_vec()).unwrap();
            offset += value_len;

            values.insert(value);
        }

        let int_range = if offset < serialized.len() {
            let start = i64::from_le_bytes(serialized[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let end = i64::from_le_bytes(serialized[offset..offset + 8].try_into().unwrap());
            offset += 8;

            Some((start, end))
        } else {
            None
        };

        let float_range = if offset < serialized.len() {
            let start = f32::from_le_bytes(serialized[offset..offset + 8].try_into().unwrap());
            offset += 4;
            let end = f32::from_le_bytes(serialized[offset..offset + 8].try_into().unwrap());
            offset += 4;

            Some((start, end))
        } else {
            None
        };

        NodeMetadata {
            values,
            int_range,
            float_range,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeMetadataIndex {
    pub data: HashMap<String, NodeMetadata>,
}

impl NodeMetadataIndex {
    pub fn new() -> Self {
        NodeMetadataIndex {
            data: HashMap::new(),
        }
    }

    pub fn from_kv_pairs(kv_pairs: Vec<&KVPair>) -> Result<Self, io::Error> {
        let mut data: HashMap<String, NodeMetadata> = HashMap::new();

        for kv_pair in kv_pairs {
            // if let Some(result) = tree
            //     .search(kv_pair.key.clone())
            //     .expect("Failed to search tree")
            // {
            //     let mut node_metadata = result.clone();
            //     node_metadata.values.insert(kv_pair.value.clone());
            //     tree.insert(kv_pair.key.clone(), node_metadata);
            // } else {
            //     let mut node_metadata = NodeMetadata {
            //         values: HashSet::new(),
            //     };
            //     node_metadata.values.insert(kv_pair.value.clone());
            //     tree.insert(kv_pair.key.clone(), node_metadata);
            // }

            match kv_pair.value.clone() {
                KVValue::String(val) => {
                    if let Some(result) = data.get(&kv_pair.key) {
                        let mut node_metadata = result.clone();
                        node_metadata.values.insert(val.clone());
                        data.insert(kv_pair.key.clone(), node_metadata);
                    } else {
                        let mut node_metadata = NodeMetadata {
                            values: HashSet::new(),
                            float_range: None,
                            int_range: None,
                        };
                        node_metadata.values.insert(val.clone());
                        data.insert(kv_pair.key.clone(), node_metadata);
                    }
                }

                KVValue::Integer(val) => {
                    if let Some(result) = data.get(&kv_pair.key) {
                        let mut node_metadata = result.clone();
                        let current_min = node_metadata.int_range.unwrap().0;
                        let current_max = node_metadata.int_range.unwrap().1;
                        let new_value = val;
                        node_metadata.int_range =
                            Some((current_min.min(new_value), current_max.max(new_value)));
                        data.insert(kv_pair.key.clone(), node_metadata);
                    } else {
                        let node_metadata = NodeMetadata {
                            values: HashSet::new(),
                            float_range: None,
                            int_range: Some((val, val)),
                        };
                        data.insert(kv_pair.key.clone(), node_metadata);
                    }
                }

                KVValue::Float(val) => {
                    if let Some(result) = data.get(&kv_pair.key.clone()) {
                        let mut node_metadata = result.clone();
                        let current_min = node_metadata.float_range.unwrap().0;
                        let current_max = node_metadata.float_range.unwrap().1;
                        let new_value = val;
                        node_metadata.float_range =
                            Some((current_min.min(new_value), current_max.max(new_value)));
                        data.insert(kv_pair.key.clone(), node_metadata);
                    } else {
                        let node_metadata = NodeMetadata {
                            values: HashSet::new(),
                            float_range: Some((val, val)),
                            int_range: None,
                        };
                        data.insert(kv_pair.key.clone(), node_metadata);
                    }
                }
            }
        }

        Ok(NodeMetadataIndex { data })
    }

    pub fn insert_kv_pair(&mut self, kv_pair: &KVPair) {
        match kv_pair.value.clone() {
            KVValue::String(val) => {
                if let Some(result) = self.data.get(&kv_pair.key.clone()) {
                    let mut node_metadata = result.clone();
                    node_metadata.values.insert(val.clone());
                    self.data.insert(kv_pair.key.clone(), node_metadata);
                } else {
                    let mut node_metadata = NodeMetadata {
                        values: HashSet::new(),
                        float_range: None,
                        int_range: None,
                    };
                    node_metadata.values.insert(val.clone());
                    self.data.insert(kv_pair.key.clone(), node_metadata);
                }
            }

            KVValue::Integer(val) => {
                if let Some(result) = self.data.get(&kv_pair.key.clone()) {
                    let mut node_metadata = result.clone();
                    let current_min = node_metadata.int_range.unwrap().0;
                    let current_max = node_metadata.int_range.unwrap().1;
                    let new_value = val;
                    node_metadata.int_range =
                        Some((current_min.min(new_value), current_max.max(new_value)));
                    self.data.insert(kv_pair.key.clone(), node_metadata);
                } else {
                    let node_metadata = NodeMetadata {
                        values: HashSet::new(),
                        float_range: None,
                        int_range: Some((val, val)),
                    };
                    self.data.insert(kv_pair.key.clone(), node_metadata);
                }
            }

            KVValue::Float(val) => {
                if let Some(result) = self.data.get(&kv_pair.key.clone()) {
                    let mut node_metadata = result.clone();
                    let current_min = node_metadata.float_range.unwrap().0;
                    let current_max = node_metadata.float_range.unwrap().1;
                    let new_value = val;
                    node_metadata.float_range =
                        Some((current_min.min(new_value), current_max.max(new_value)));
                    self.data.insert(kv_pair.key.clone(), node_metadata);
                } else {
                    let node_metadata = NodeMetadata {
                        values: HashSet::new(),
                        float_range: Some((val, val)),
                        int_range: None,
                    };
                    self.data.insert(kv_pair.key.clone(), node_metadata);
                }
            }
        }
    }

    pub fn get_all_values(&self) -> Vec<(&String, &NodeMetadata)> {
        let mut all_values = Vec::new();

        for (key, node_metadata) in self.data.iter() {
            all_values.push((key, node_metadata));
        }

        all_values
    }

    pub fn get(&self, key: String) -> Option<&NodeMetadata> {
        self.data.get(&key)
    }

    pub fn insert(&mut self, key: String, node_metadata: NodeMetadata) {
        self.data.insert(key, node_metadata);
    }
}
