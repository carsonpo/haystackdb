use std::fmt::Display;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::structures::mmap_tree::Tree;

use super::metadata_index::KVPair;
use super::mmap_tree::serialization::{TreeDeserialization, TreeSerialization};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InvertedIndexItem {
    pub indices: Vec<usize>,
    pub ids: Vec<u128>,
}

impl Display for InvertedIndexItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InvertedIndexItem {{ ... }}")
    }
}

impl TreeSerialization for InvertedIndexItem {
    fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();

        serialized.extend_from_slice(self.indices.len().to_le_bytes().as_ref());

        let len_of_index_bytes: usize = 8;

        serialized.extend_from_slice(len_of_index_bytes.to_le_bytes().as_ref());

        for index in &self.indices {
            serialized.extend_from_slice(index.to_le_bytes().as_ref());
        }

        serialized.extend_from_slice(self.ids.len().to_le_bytes().as_ref());

        let len_of_id_bytes: usize = 16;

        serialized.extend_from_slice(len_of_id_bytes.to_le_bytes().as_ref());

        for id in &self.ids {
            serialized.extend_from_slice(id.to_le_bytes().as_ref());
        }

        serialized
    }
}

impl TreeDeserialization for InvertedIndexItem {
    fn deserialize(data: &[u8]) -> Self {
        let mut offset = 0;

        let indices_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;
        // let mut indices = Vec::new();
        let len_of_index_bytes = usize::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let start = offset;
        let end = start + indices_len * len_of_index_bytes;

        let indices_bytes = &data[start..end];

        let indices_chunks = indices_bytes.chunks(len_of_index_bytes);

        // for chunk in indices_chunks {
        //     let index = usize::from_le_bytes(chunk.try_into().unwrap());
        //     indices.push(index);
        // }

        let indices = indices_chunks
            .map(|chunk| usize::from_le_bytes(chunk.try_into().unwrap()))
            .collect();

        offset = end;

        let ids_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;
        // let mut ids = Vec::new();
        let len_of_id_bytes = usize::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // get them all and split the bytes into chunks

        let start = offset;
        let end = start + ids_len * len_of_id_bytes;
        let ids_bytes = &data[start..end];

        let ids_chunks = ids_bytes.chunks(len_of_id_bytes);

        // for chunk in ids_chunks {
        //     let id = String::from_utf8(chunk.to_vec()).unwrap();
        //     ids.push(id);
        // }
        let ids = ids_chunks
            .map(|chunk| u128::from_le_bytes(chunk.try_into().unwrap()))
            .collect();

        InvertedIndexItem { indices, ids }
    }
}

pub struct InvertedIndex {
    pub path: PathBuf,
    pub tree: Tree<KVPair, InvertedIndexItem>,
}

fn compress_indices(indices: Vec<usize>) -> Vec<usize> {
    let mut compressed = Vec::new();
    if indices.is_empty() {
        return compressed;
    }

    let mut current_start = indices[0];
    let mut count = 1;

    for i in 1..indices.len() {
        if indices[i] == current_start + count {
            count += 1;
        } else {
            compressed.push(count);
            compressed.push(current_start);
            current_start = indices[i];
            count = 1;
        }
    }
    // Don't forget to push the last sequence
    compressed.push(count);
    compressed.push(current_start);

    compressed
}

fn decompress_indices(compressed: Vec<usize>) -> Vec<usize> {
    let mut decompressed = Vec::new();
    let mut i = 0;

    while i < compressed.len() / 2 {
        let count = compressed[i];
        let start = compressed[i + 1];
        decompressed.extend((start..start + count).collect::<Vec<usize>>());
        i += 2;
    }

    decompressed
}

impl InvertedIndex {
    pub fn new(path: PathBuf) -> Self {
        let tree = Tree::new(path.clone()).expect("Failed to create tree");
        InvertedIndex { path, tree }
    }

    pub fn insert(&mut self, key: KVPair, value: InvertedIndexItem) {
        // println!("Inserting INTO INVERTED INDEX: {:?}", key);
        self.tree.insert(key, value).expect("Failed to insert");
    }

    pub fn get(&mut self, key: KVPair) -> Option<InvertedIndexItem> {
        // println!("Getting key: {:?}", key);
        match self.tree.search(key) {
            Ok(v) => {
                // decompress the indices
                match v {
                    Some(mut item) => {
                        item.indices = decompress_indices(item.indices);
                        Some(item)
                    }
                    None => None,
                }
            }
            Err(_) => None,
        }
    }

    pub fn insert_append(&mut self, key: KVPair, mut value: InvertedIndexItem) {
        match self.get(key.clone()) {
            Some(mut v) => {
                // v.indices.extend(value.indices);
                v.ids.extend(value.ids);

                let mut decompressed = v.indices.clone();

                // binary search to insert all of the ones to append
                for index in value.indices {
                    let idx = decompressed.binary_search(&index).unwrap_or_else(|x| x);
                    decompressed.insert(idx, index);
                }

                println!("Compressed: {:?}", decompressed.len());

                v.indices = compress_indices(decompressed);

                self.insert(key, v);
            }
            None => {
                value.indices = compress_indices(value.indices);
                println!("Compressed: {:?}", value.indices.len());
                self.insert(key, value);
            }
        }
    }
}
