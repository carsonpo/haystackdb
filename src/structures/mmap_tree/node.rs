use std::{fmt::Debug, io};

use super::serialization::{TreeDeserialization, TreeSerialization};

#[derive(Debug, PartialEq, Clone)]
pub enum NodeType {
    Leaf,
    Internal,
}

const MAX_KEYS: usize = 32;

pub fn serialize_node_type(node_type: &NodeType) -> [u8; 1] {
    match node_type {
        NodeType::Leaf => [0],
        NodeType::Internal => [1],
    }
}

pub fn deserialize_node_type(data: &[u8]) -> NodeType {
    match data[0] {
        0 => NodeType::Leaf,
        1 => NodeType::Internal,
        _ => panic!("Invalid node type"),
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Node<K, V> {
    pub keys: Vec<K>,
    pub values: Vec<Option<V>>, // Option for handling deletion in COW
    pub children: Vec<usize>,   // Offsets into the memmap file
    pub max_keys: usize,        // Maximum number of keys a node can hold
    pub node_type: NodeType,
    pub offset: usize, // Offset into the memmap file
    pub is_root: bool,
    pub parent_offset: Option<usize>, // Offset of the parent node
}

impl<K, V> Node<K, V>
where
    K: Clone + Ord + TreeSerialization + TreeDeserialization,
    V: Clone + TreeSerialization + TreeDeserialization,
{
    pub fn new_leaf(offset: usize) -> Self {
        Node {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            max_keys: MAX_KEYS, // Assuming a small number for testing purposes
            node_type: NodeType::Leaf,
            offset,
            is_root: false,
            parent_offset: Some(0),
        }
    }

    pub fn new_internal(offset: usize) -> Self {
        Node {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            max_keys: MAX_KEYS,
            node_type: NodeType::Internal,
            offset,
            is_root: false,
            parent_offset: Some(0),
        }
    }

    pub fn split(&mut self, b: usize) -> Result<(K, Node<K, V>), io::Error> {
        // println!("Splitting node: {:?}", self.keys);

        match self.node_type {
            NodeType::Internal => {
                if b <= 1 || b > self.keys.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Invalid split point for internal node",
                    ));
                }
                let mut sibling_keys = self.keys.split_off(b - 1);
                let median_key = sibling_keys.remove(0);

                let sibling_children = self.children.split_off(b);

                let sibling = Node {
                    keys: sibling_keys,
                    values: Vec::new(),
                    children: sibling_children,
                    max_keys: self.max_keys,
                    node_type: NodeType::Internal,
                    offset: 0, // This should be set when the node is stored
                    is_root: false,
                    parent_offset: self.parent_offset,
                };

                // println!(
                //     "Internal node split: median_key = {}, sibling_keys = {:?}",
                //     median_key, sibling.keys
                // );
                Ok((median_key, sibling))
            }
            NodeType::Leaf => {
                if b < 1 || b >= self.keys.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Invalid split point for leaf node",
                    ));
                }
                let sibling_keys = self.keys.split_off(b);
                let median_key = self.keys.get(b - 1).unwrap().clone();
                let sibling_values = self.values.split_off(b);

                let sibling = Node {
                    keys: sibling_keys,
                    values: sibling_values,
                    children: Vec::new(),
                    max_keys: self.max_keys,
                    node_type: NodeType::Leaf,
                    offset: 0, // This should be set when the node is stored
                    is_root: false,
                    parent_offset: self.parent_offset,
                };

                // println!(
                //     "Leaf node split: median_key = {}, sibling_keys = {:?}",
                //     median_key, sibling.keys
                // );
                Ok((median_key, sibling))
            }
        }
    }

    pub fn is_full(&self) -> bool {
        let b = self.max_keys;
        return self.keys.len() >= (2 * b - 1);
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::with_capacity(1_000_000);
        serialized.extend_from_slice(&serialize_node_type(&self.node_type));
        serialized.push(self.is_root as u8);
        serialized.extend_from_slice(&(self.parent_offset.unwrap_or(0) as u64).to_le_bytes());
        serialize_length(&mut serialized, self.keys.len() as u32);
        serialize_length(&mut serialized, self.values.len() as u32);
        serialize_length(&mut serialized, self.children.len() as u32);

        for key in &self.keys {
            let serialized_key = key.serialize();
            serialized.extend_from_slice(&serialize_length(
                &mut Vec::new(),
                serialized_key.len() as u32,
            ));
            serialized.extend_from_slice(&serialized_key);
        }

        for value in &self.values {
            match value {
                Some(value) => {
                    let serialized_value = value.serialize();
                    serialized.extend_from_slice(&serialize_length(
                        &mut Vec::new(),
                        serialized_value.len() as u32,
                    ));
                    serialized.extend_from_slice(&serialized_value);
                }
                None => serialized.extend_from_slice(&0u32.to_le_bytes()),
            }
        }

        for child in &self.children {
            serialized.extend_from_slice(&child.to_le_bytes());
        }

        serialized
    }

    pub fn deserialize(data: &[u8]) -> Self {
        let mut offset = 0;

        let node_type = deserialize_node_type(&data[offset..offset + 1]);
        offset += 1;
        let is_root = data[offset] == 1;
        offset += 1;
        let parent_offset =
            u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;
        let keys_len = read_length(&data[offset..offset + 4]);
        offset += 4;
        let values_len = read_length(&data[offset..offset + 4]);
        offset += 4;
        let num_children = read_length(&data[offset..offset + 4]);
        offset += 4;

        let mut keys = Vec::with_capacity(keys_len);
        for _ in 0..keys_len {
            let key_size = read_length(&data[offset..offset + 4]) as usize;
            offset += 4;
            let key = K::deserialize(&data[offset..offset + key_size]);
            offset += key_size;
            keys.push(key);
        }

        let mut values = Vec::with_capacity(values_len);
        for _ in 0..values_len {
            let value_size = read_length(&data[offset..offset + 4]) as usize;
            offset += 4;
            let value = if value_size > 0 {
                Some(V::deserialize(&data[offset..offset + value_size]))
            } else {
                None
            };
            offset += value_size;
            values.push(value);
        }

        let mut children = Vec::with_capacity(num_children);
        for _ in 0..num_children {
            let child_offset = usize::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;
            children.push(child_offset);
        }

        Node {
            keys,
            values,
            children,
            max_keys: MAX_KEYS,
            node_type,
            offset: 0,
            is_root,
            parent_offset: Some(parent_offset),
        }
    }

    pub fn set_key_value(&mut self, key: K, value: V) {
        let idx = self.keys.binary_search(&key).unwrap_or_else(|x| x);
        self.keys.insert(idx, key);
        self.values.insert(idx, Some(value));
    }

    pub fn get_value(&self, key: K) -> Option<V> {
        match self.keys.binary_search(&key) {
            Ok(idx) => self.values[idx].clone(),
            Err(_) => None,
        }
    }
}
impl<K, V> Default for Node<K, V> {
    fn default() -> Self {
        Node {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            max_keys: 0,               // Adjust this as necessary
            node_type: NodeType::Leaf, // Or another appropriate default NodeType
            offset: 0,
            is_root: false,
            parent_offset: None,
        }
    }
}

fn serialize_length(buffer: &mut Vec<u8>, length: u32) -> &Vec<u8> {
    buffer.extend_from_slice(&length.to_le_bytes());

    // Return the buffer to allow chaining
    buffer
}

fn read_length(data: &[u8]) -> usize {
    u32::from_le_bytes(data.try_into().unwrap()) as usize
}
