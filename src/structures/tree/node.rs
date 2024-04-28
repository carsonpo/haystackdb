use std::{
    fmt::{Debug, Display},
    io,
};

use super::serialization::{TreeDeserialization, TreeSerialization};

#[derive(Debug, PartialEq, Clone)]
pub enum NodeType {
    Leaf,
    Internal,
}

const MAX_KEYS: usize = 10;

pub struct Node<K, V> {
    pub keys: Vec<K>,
    pub values: Vec<Option<V>>, // Option for handling deletion in COW
    pub children: Vec<Box<Node<K, V>>>, // Using Box for heap allocation
    pub max_keys: usize,        // Maximum number of keys a node can hold
    pub node_type: NodeType,
}

impl<K, V> Node<K, V>
where
    K: Clone + Ord + TreeSerialization + TreeDeserialization + Display + Debug + Copy,
    V: Clone + TreeSerialization + TreeDeserialization + Display + Debug,
{
    pub fn new_leaf() -> Self {
        Node {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            max_keys: MAX_KEYS, // Assuming a small number for testing purposes
            node_type: NodeType::Leaf,
        }
    }

    pub fn new_internal() -> Self {
        Node {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            max_keys: MAX_KEYS,
            node_type: NodeType::Internal,
        }
    }

    pub fn clone(&self) -> Self {
        Node {
            keys: self.keys.clone(),
            values: self.values.clone(),
            children: self
                .children
                .iter()
                .map(|c| Box::new((**c).clone()))
                .collect(),
            max_keys: self.max_keys,
            node_type: self.node_type.clone(),
        }
    }

    pub fn split(&mut self) -> Result<(K, Node<K, V>), io::Error> {
        match self.node_type {
            NodeType::Internal => {
                let split_index = (self.keys.len() + 1) / 2;
                let median_key = self.keys[split_index].clone();

                let sibling_keys = self.keys.split_off(split_index + 1);
                let sibling_children = self.children.split_off(split_index + 1);

                let sibling = Node {
                    keys: sibling_keys,
                    values: Vec::new(),
                    children: sibling_children,
                    max_keys: self.max_keys,
                    node_type: NodeType::Internal,
                };

                self.keys.pop();

                Ok((median_key, sibling))
            }
            NodeType::Leaf => {
                let split_index = (self.keys.len() + 1) / 2;
                let median_key = self.keys[split_index].clone();

                let sibling_keys = self.keys.split_off(split_index);
                let sibling_values = self.values.split_off(split_index);

                let sibling = Node {
                    keys: sibling_keys,
                    values: sibling_values,
                    children: Vec::new(),
                    max_keys: self.max_keys,
                    node_type: NodeType::Leaf,
                };

                Ok((median_key, sibling))
            }
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
        }
    }
}
