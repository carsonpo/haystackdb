use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use super::ann_tree::metadata::{NodeMetadata, NodeMetadataIndex};
use crate::structures::mmap_tree::serialization::{TreeDeserialization, TreeSerialization};
use std::fmt::Display;
use std::hash::{Hash, Hasher};

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
#[serde(tag = "type", content = "args")]
pub enum Filter {
    And(Vec<Filter>),
    Or(Vec<Filter>),
    In(String, Vec<String>),
    Eq(String, String),
    Gt(String, f64),  // Greater than
    Gte(String, f64), // Greater than or equal
    Lt(String, f64),  // Less than
    Lte(String, f64), // Less than or equal
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Query {
    filters: Filter,
}

pub struct Filters {
    pub metadata: HashMap<String, HashSet<String>>,
}

impl Filters {
    pub fn new(metadata: HashMap<String, HashSet<String>>) -> Self {
        Filters { metadata }
    }

    // pub fn matches(&self, filters: &Filter) -> bool {
    //     match filters {
    //         Filter::And(filters) => filters.par_iter().all(|f| self.matches(f)),
    //         Filter::Or(filters) => filters.par_iter().any(|f| self.matches(f)),
    //         Filter::In(key, values) => match self.metadata.get(key) {
    //             Some(set) => values.iter().any(|v| set.contains(v)),
    //             None => false,
    //         },
    //         Filter::Eq(key, value) => match self.metadata.get(key) {
    //             Some(set) => set.contains(value),
    //             None => false,
    //         },
    //     }
    // }

    pub fn should_prune(filter: &Filter, node_metadata: &NodeMetadataIndex) -> bool {
        match filter {
            Filter::And(filters) => filters
                .par_iter()
                .all(|f| Filters::should_prune(f, node_metadata)),
            Filter::Or(filters) => filters
                .par_iter()
                .any(|f| Filters::should_prune(f, node_metadata)),
            Filter::In(key, values) => match node_metadata.get(key.to_string()) {
                Some(node_values) => !values.par_iter().any(|v| node_values.values.contains(v)),
                None => true,
            },
            Filter::Eq(key, value) => match node_metadata.get(key.to_string()) {
                Some(node_values) => !node_values.values.contains(value),
                None => true,
            },
            Filter::Gt(key, value) => match node_metadata.get(key.to_string()) {
                Some(node_values) => match node_values.float_range {
                    Some((min, _)) => min > (*value as f32),
                    None => match node_values.int_range {
                        Some((min, _)) => min > (*value as i64),
                        None => true,
                    },
                },
                None => true,
            },
            Filter::Gte(key, value) => match node_metadata.get(key.to_string()) {
                Some(node_values) => match node_values.float_range {
                    Some((min, _)) => min >= (*value as f32),
                    None => match node_values.int_range {
                        Some((min, _)) => min >= (*value as i64),
                        None => true,
                    },
                },
                None => true,
            },
            Filter::Lt(key, value) => match node_metadata.get(key.to_string()) {
                Some(node_values) => match node_values.float_range {
                    Some((_, max)) => max < (*value as f32),
                    None => match node_values.int_range {
                        Some((_, max)) => max < (*value as i64),
                        None => true,
                    },
                },
                None => true,
            },
            Filter::Lte(key, value) => match node_metadata.get(key.to_string()) {
                Some(node_values) => match node_values.float_range {
                    Some((_, max)) => max <= (*value as f32),
                    None => match node_values.int_range {
                        Some((_, max)) => max <= (*value as i64),
                        None => true,
                    },
                },
                None => true,
            },
        }
    }

    pub fn should_prune_metadata(filter: &Filter, metadata: &Vec<KVPair>) -> bool {
        let node_metadata: NodeMetadataIndex = metadata
            .into_iter()
            .map(|kv_pair| (kv_pair.key.clone(), kv_pair.value.clone()))
            .fold(NodeMetadataIndex::new(), |mut acc, (key, value)| {
                match acc.get(key.clone()) {
                    Some(node_values) => match value {
                        KVValue::String(v) => {
                            let mut node_values = node_values.clone();
                            node_values.values.insert(v);
                            acc.insert(key, node_values);
                        }
                        KVValue::Float(v) => {
                            let mut node_values = node_values.clone();
                            let float_range = match node_values.float_range {
                                Some((min, max)) => Some((min.min(v), max.max(v))),
                                None => Some((v, v)),
                            };
                            node_values.float_range = float_range;
                            acc.insert(key, node_values);
                        }
                        KVValue::Integer(v) => {
                            let mut node_values = node_values.clone();
                            let int_range = match node_values.int_range {
                                Some((min, max)) => Some((min.min(v as i64), max.max(v as i64))),
                                None => Some((v as i64, v as i64)),
                            };
                            node_values.int_range = int_range;
                            acc.insert(key, node_values);
                        }
                    },
                    None => {
                        let mut node_values = NodeMetadata {
                            float_range: None,
                            int_range: None,
                            values: HashSet::new(),
                        };
                        match value {
                            KVValue::String(v) => {
                                node_values.values.insert(v);
                            }
                            KVValue::Float(v) => {
                                node_values.float_range = Some((v, v));
                            }
                            KVValue::Integer(v) => {
                                node_values.int_range = Some((v as i64, v as i64));
                            }
                        }

                        acc.insert(key, node_values);
                    }
                }

                acc
            });

        Filters::should_prune(filter, &node_metadata)
    }
}

pub fn combine_filters(filters: Vec<NodeMetadataIndex>) -> NodeMetadataIndex {
    let mut result = NodeMetadataIndex::new();

    for filter in filters {
        for (key, values) in filter.get_all_values() {
            match result.get(key.clone()) {
                Some(node_values) => {
                    let mut node_values = node_values.clone();
                    node_values.values.extend(values.values.iter().cloned());

                    let float_range = match (node_values.float_range, values.float_range) {
                        (Some((min1, max1)), Some((min2, max2))) => {
                            Some((min1.min(min2), max1.max(max2)))
                        }
                        (Some((min1, max1)), None) => Some((min1, max1)),
                        (None, Some((min2, max2))) => Some((min2, max2)),
                        _ => None,
                    };

                    let int_range = match (node_values.int_range, values.int_range) {
                        (Some((min1, max1)), Some((min2, max2))) => {
                            Some((min1.min(min2), max1.max(max2)))
                        }
                        (Some((min1, max1)), None) => Some((min1, max1)),
                        (None, Some((min2, max2))) => Some((min2, max2)),
                        _ => None,
                    };

                    node_values.float_range = float_range;
                    node_values.int_range = int_range;

                    result.insert(key.clone(), node_values);
                }
                None => {
                    let mut node_values = NodeMetadata {
                        float_range: None,
                        int_range: None,
                        values: HashSet::new(),
                    };
                    node_values.values.extend(values.values.iter().cloned());
                    node_values.float_range = values.float_range;
                    node_values.int_range = values.int_range;

                    result.insert(key.clone(), node_values);
                }
            }
        }
    }

    result
}

pub fn calc_metadata_index_for_metadata(kvs: Vec<Vec<KVPair>>) -> NodeMetadataIndex {
    let node_metadata: NodeMetadataIndex = kvs
        .into_iter()
        .map(|metadata| {
            metadata
                .into_iter()
                .map(|kv_pair| (kv_pair.key.clone(), kv_pair.value.clone()))
                .fold(NodeMetadataIndex::new(), |mut acc, (key, value)| {
                    match acc.get(key.clone()) {
                        Some(node_values) => match value {
                            KVValue::String(v) => {
                                let mut node_values = node_values.clone();
                                node_values.values.insert(v);
                                acc.insert(key, node_values);
                            }
                            KVValue::Float(v) => {
                                let mut node_values = node_values.clone();
                                let float_range = match node_values.float_range {
                                    Some((min, max)) => Some((min.min(v), max.max(v))),
                                    None => Some((v, v)),
                                };
                                node_values.float_range = float_range;
                                acc.insert(key, node_values);
                            }
                            KVValue::Integer(v) => {
                                let mut node_values = node_values.clone();
                                let int_range = match node_values.int_range {
                                    Some((min, max)) => {
                                        Some((min.min(v as i64), max.max(v as i64)))
                                    }
                                    None => Some((v as i64, v as i64)),
                                };
                                node_values.int_range = int_range;
                                acc.insert(key, node_values);
                            }
                        },
                        None => {
                            let mut node_values = NodeMetadata {
                                float_range: None,
                                int_range: None,
                                values: HashSet::new(),
                            };
                            match value {
                                KVValue::String(v) => {
                                    node_values.values.insert(v);
                                }
                                KVValue::Float(v) => {
                                    node_values.float_range = Some((v, v));
                                }
                                KVValue::Integer(v) => {
                                    node_values.int_range = Some((v as i64, v as i64));
                                }
                            }

                            acc.insert(key, node_values);
                        }
                    }

                    acc
                })
        })
        .fold(NodeMetadataIndex::new(), |mut acc, metadata| {
            for (key, values) in metadata.get_all_values() {
                match acc.get(key.clone()) {
                    Some(node_values) => {
                        let mut node_values = node_values.clone();
                        node_values.values.extend(values.values.iter().cloned());

                        let float_range = match (node_values.float_range, values.float_range) {
                            (Some((min1, max1)), Some((min2, max2))) => {
                                Some((min1.min(min2), max1.max(max2)))
                            }
                            (Some((min1, max1)), None) => Some((min1, max1)),
                            (None, Some((min2, max2))) => Some((min2, max2)),
                            _ => None,
                        };

                        let int_range = match (node_values.int_range, values.int_range) {
                            (Some((min1, max1)), Some((min2, max2))) => {
                                Some((min1.min(min2), max1.max(max2)))
                            }
                            (Some((min1, max1)), None) => Some((min1, max1)),
                            (None, Some((min2, max2))) => Some((min2, max2)),
                            _ => None,
                        };

                        node_values.float_range = float_range;

                        node_values.int_range = int_range;

                        acc.insert(key.clone(), node_values);
                    }
                    None => {
                        let mut node_values = NodeMetadata {
                            float_range: None,
                            int_range: None,
                            values: HashSet::new(),
                        };
                        node_values.values.extend(values.values.iter().cloned());
                        node_values.float_range = values.float_range;
                        node_values.int_range = values.int_range;

                        acc.insert(key.clone(), node_values);
                    }
                }
            }

            acc
        });

    node_metadata
}
