pub mod node;
pub mod serialization;

use std::fmt::{Debug, Display};
use std::io;

use node::{Node, NodeType};
use serialization::{TreeDeserialization, TreeSerialization};

pub struct Tree<K, V> {
    pub root: Box<Node<K, V>>,
    pub b: usize,
}

impl<K, V> Tree<K, V>
where
    K: Clone + Ord + TreeSerialization + TreeDeserialization + Display + Debug + Copy,
    V: Clone + TreeSerialization + TreeDeserialization + Display + Debug,
{
    pub fn new() -> Self {
        Tree {
            root: Box::new(Node::new_leaf()), // Initially the root is a leaf node
            b: 4,
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<(), io::Error> {
        let mut root = std::mem::replace(&mut self.root, Box::new(Node::new_leaf()));
        if self.is_node_full(&root)? {
            let mut new_root = Node::new_internal();
            let (median, sibling) = root.split()?;
            new_root.keys.push(median);
            new_root.children.push(root);
            new_root.children.push(Box::new(sibling));
            root = Box::new(new_root);
        }
        self.insert_non_full(&mut *root, key, value)?;
        self.root = root;
        Ok(())
    }

    fn insert_non_full(
        &mut self,
        node: &mut Node<K, V>,
        key: K,
        value: V,
    ) -> Result<(), io::Error> {
        match &mut node.node_type {
            NodeType::Leaf => {
                let idx = node.keys.binary_search(&key).unwrap_or_else(|x| x);
                node.keys.insert(idx, key);
                node.values.insert(idx, Some(value));
                Ok(())
            }
            NodeType::Internal => {
                let idx = node.keys.binary_search(&key).unwrap_or_else(|x| x);
                let child_idx = if idx == node.keys.len() || key < node.keys[idx] {
                    idx
                } else {
                    idx + 1
                };

                if self.is_node_full(&node.children[child_idx])? {
                    let (median, sibling) = node.children[child_idx].split()?;
                    node.keys.insert(idx, median);
                    node.children.insert(child_idx + 1, Box::new(sibling));
                    if key >= node.keys[idx] {
                        self.insert_non_full(&mut *node.children[child_idx + 1], key, value)
                    } else {
                        self.insert_non_full(&mut *node.children[child_idx], key, value)
                    }
                } else {
                    self.insert_non_full(&mut *node.children[child_idx], key, value)
                }
            }
        }
    }

    fn is_node_full(&self, node: &Node<K, V>) -> Result<bool, io::Error> {
        Ok(node.keys.len() == node.max_keys)
    }

    pub fn search(&self, key: K) -> Result<Option<V>, io::Error> {
        self.search_node(&*self.root, key)
    }

    fn search_node(&self, node: &Node<K, V>, key: K) -> Result<Option<V>, io::Error> {
        match node.node_type {
            NodeType::Internal => {
                let idx = node.keys.binary_search(&key).unwrap_or_else(|x| x);
                if idx < node.keys.len() && node.keys[idx] == key {
                    self.search_node(&node.children[idx + 1], key)
                } else {
                    self.search_node(&node.children[idx], key)
                }
            }
            NodeType::Leaf => match node.keys.binary_search(&key) {
                Ok(idx) => Ok(node.values.get(idx).expect("could not get value").clone()),
                Err(_) => Ok(None),
            },
        }
    }
}
