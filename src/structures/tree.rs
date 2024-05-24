pub mod node;

use std::fmt::{Debug, Display};
use std::io;
use std::path::PathBuf;

use node::{Node, NodeType, NodeValue};

use super::ann_tree::serialization::{TreeDeserialization, TreeSerialization};
// use super::block_storage::StorageLayer;
use super::storage_layer::StorageLayer;

pub struct Tree<K, V> {
    pub storage: StorageLayer,
    phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Tree<K, V>
where
    K: Clone + Ord + TreeSerialization + TreeDeserialization + Debug + Display,
    V: Clone + TreeSerialization + TreeDeserialization,
{
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let mut storage = StorageLayer::new(path)?;

        if storage.used_blocks() <= 1 {
            let root_offset: usize;
            let mut root: Node<K, V> = Node::new_leaf(0);
            root.is_root = true;

            let serialized_root = root.serialize();

            root_offset = storage.store(serialized_root, 0)?;

            println!("Root offset: {}", root_offset);
            storage.set_root_offset(root_offset);
        }

        Ok(Tree {
            storage,
            phantom: std::marker::PhantomData,
        })
    }

    pub fn store_node(&mut self, node: &mut Node<K, V>) -> io::Result<usize> {
        let serialized_node = node.serialize();
        println!("Storing node: {:?}", node.offset);
        println!("Node has {} keys", node.keys.len());
        let offset = self.storage.store(serialized_node, node.offset)?;
        node.offset = offset;

        Ok(offset)
    }

    pub fn load_node(&self, offset: usize) -> io::Result<Node<K, V>> {
        let serialized_node = self.storage.load(offset)?;
        let mut node = Node::<K, V>::deserialize(&serialized_node);
        node.offset = offset;
        Ok(node)
    }

    pub fn get(&self, key: &K) -> io::Result<Option<NodeValue<V>>> {
        let root_offset = self.storage.root_offset();
        let root = self.load_node(root_offset)?;

        println!("Root offset: {}", root_offset);
        println!("Root keys: {:?}", root.keys);

        let result = self.get_recursive(&root, key);

        Ok(result)
    }

    fn get_recursive(&self, node: &Node<K, V>, key: &K) -> Option<NodeValue<V>> {
        match node.node_type {
            NodeType::Leaf => {
                for i in 0..node.keys.len() {
                    if node.keys[i] == *key {
                        println!("Found key: {}", key);
                        return node.values[i].clone();
                    }
                }
                None
            }
            NodeType::Internal => {
                let mut i = 0;
                while i < node.keys.len() && *key > node.keys[i] {
                    i += 1;
                }

                println!("Searching in child: {}", i);

                let child_offset = node.children[i];
                let child = self.load_node(child_offset).unwrap();

                self.get_recursive(&child, key)
            }
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<(), io::Error> {
        // println!("Inserting key: {}, value: {}", key, value);
        let vals = vec![(key, value)];

        self.batch_insert(vals)
    }

    pub fn batch_insert(&mut self, entries: Vec<(K, V)>) -> Result<(), io::Error> {
        if entries.is_empty() {
            println!("No entries to insert");
            return Ok(());
        }

        let mut entries = entries;
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let entrypoint = self.find_entrypoint(&entries[0].0)?;

        let mut current_node = entrypoint;

        for (key, value) in entries.iter() {
            if current_node.is_full() {
                let (median, mut sibling) = current_node.split(crate::constants::B)?;
                let sibling_offset = self.store_node(&mut sibling)?;
                self.store_node(&mut current_node)?; // Store changes to the original node after splitting
                if current_node.is_root {
                    let mut new_root = Node::new_internal(0);
                    new_root.is_root = true;
                    new_root.keys.push(median.clone());
                    new_root.children.push(current_node.offset); // old root offset
                    new_root.children.push(sibling_offset); // new sibling offset
                    new_root.parent_offset = None;
                    let new_root_offset = self.store_node(&mut new_root)?;
                    self.storage.set_root_offset(new_root_offset);
                    current_node.is_root = false;
                    current_node.parent_offset = Some(new_root_offset);
                    sibling.parent_offset = Some(new_root_offset);
                    self.store_node(&mut current_node)?;
                    self.store_node(&mut sibling)?;
                    self.storage.set_root_offset(new_root_offset);
                } else {
                    let parent_offset = current_node.parent_offset.unwrap();
                    let mut parent = self.load_node(parent_offset)?;
                    let idx = parent
                        .keys
                        .binary_search(&median.clone())
                        .unwrap_or_else(|x| x);
                    parent
                        .keys
                        .insert(idx.min(parent.children.len() - 1), median.clone());
                    parent
                        .children
                        .insert((idx + 1).min(parent.children.len() - 1), sibling_offset);
                    self.store_node(&mut parent)?;
                }

                if *key >= median {
                    current_node = sibling;
                }
            }

            // Insert the key into the correct leaf node
            let position = current_node
                .keys
                .binary_search(key)
                .unwrap_or_else(|x| x)
                .min(current_node.keys.len() - 1);

            if current_node.keys.get(position) == Some(&key) {
                current_node.values[position] =
                    Some(NodeValue::new(value.clone(), &mut self.storage)?);
            } else {
                current_node.keys.insert(position, key.clone());
                current_node.values.insert(
                    position,
                    Some(NodeValue::new(value.clone(), &mut self.storage)?),
                );
            }
            self.store_node(&mut current_node)?; // Store changes after each insertion
        }

        Ok(())
    }

    pub fn find_entrypoint(&self, key: &K) -> io::Result<Node<K, V>> {
        let root_offset = self.storage.root_offset();
        let root = self.load_node(root_offset)?;

        let mut current_node = root;
        loop {
            match current_node.node_type {
                NodeType::Leaf => {
                    return Ok(current_node);
                }
                NodeType::Internal => {
                    let i = current_node.keys.binary_search(key).unwrap_or_else(|x| x);

                    let child_offset = current_node.children[i.min(current_node.keys.len() - 1)];
                    current_node = self.load_node(child_offset)?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn test_tree_insert() {
        let path = PathBuf::from("test_tree_insert.bin");
        let _ = fs::remove_dir_all(&path);

        let mut tree: Tree<i32, i32> = Tree::new(path).unwrap();

        let entries = vec![(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)];
        tree.batch_insert(entries).unwrap();

        let result = tree
            .get(&1)
            .unwrap()
            .unwrap()
            .get(&tree.storage)
            .expect("Value not found");
        assert_eq!(result, 1);

        let result = tree
            .get(&2)
            .unwrap()
            .unwrap()
            .get(&tree.storage)
            .expect("Value not found");
        assert_eq!(result, 2);

        let result = tree
            .get(&3)
            .unwrap()
            .unwrap()
            .get(&tree.storage)
            .expect("Value not found");

        assert_eq!(result, 3);

        let result = tree
            .get(&4)
            .unwrap()
            .unwrap()
            .get(&tree.storage)
            .expect("Value not found");

        assert_eq!(result, 4);

        let result = tree
            .get(&5)
            .unwrap()
            .unwrap()
            .get(&tree.storage)
            .expect("Value not found");

        assert_eq!(result, 5);

        let result = tree.get(&6).unwrap();

        assert_eq!(result, None);

        let result = tree.get(&0).unwrap();

        assert_eq!(result, None);
    }
}
