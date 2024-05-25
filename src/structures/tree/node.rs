use crate::structures::{
    ann_tree::serialization::{TreeDeserialization, TreeSerialization},
    block_storage::BlockStorage,
    storage_layer::StorageLayer,
};
use std::io;

#[derive(Debug, PartialEq, Clone)]
pub enum NodeType {
    Leaf,
    Internal,
}

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

fn serialize_length(buffer: &mut Vec<u8>, length: u32) -> &Vec<u8> {
    buffer.extend_from_slice(&length.to_le_bytes());

    // Return the buffer to allow chaining
    buffer
}

fn read_length(data: &[u8]) -> usize {
    u32::from_le_bytes(data.try_into().unwrap()) as usize
}

#[derive(Debug, PartialEq, Clone)]
pub struct NodeValue<T> {
    offset: usize,
    value: Option<T>,
}

impl<T> NodeValue<T>
where
    T: Clone + TreeDeserialization + TreeSerialization,
{
    pub fn get(&mut self, storage: &StorageLayer) -> Result<T, io::Error> {
        match self.value.clone() {
            Some(value) => Ok(value),
            None => {
                let bytes = storage.load(self.offset)?;
                let value = T::deserialize(&bytes);
                self.value = Some(value.clone());
                Ok(value)
            }
        }
    }

    pub fn new(value: T, storage: &mut StorageLayer) -> Result<Self, io::Error> {
        let offset = storage.store(value.serialize(), 0)?;
        Ok(NodeValue {
            offset,
            value: Some(value),
        })
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Node<K, V> {
    pub keys: Vec<K>,
    pub values: Vec<Option<NodeValue<V>>>,
    pub children: Vec<usize>,
    pub node_type: NodeType,
    pub offset: usize,
    pub is_root: bool,
    pub parent_offset: Option<usize>,
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
                    node_type: NodeType::Internal,
                    offset: 0,
                    is_root: false,
                    parent_offset: self.parent_offset,
                };

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
                    node_type: NodeType::Leaf,
                    offset: 0,
                    is_root: false,
                    parent_offset: self.parent_offset,
                };

                Ok((median_key, sibling))
            }
        }
    }

    pub fn is_full(&self) -> bool {
        let b = crate::constants::B;
        return self.keys.len() >= (2 * b - 1);
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        buffer.extend_from_slice(&serialize_node_type(&self.node_type));
        buffer.extend_from_slice(&(self.is_root as u8).to_le_bytes());
        match &self.parent_offset {
            Some(parent_offset) => {
                buffer.extend_from_slice(&(*parent_offset as u64).to_le_bytes());
            }
            None => {
                buffer.extend_from_slice(&0u64.to_le_bytes());
            }
        }

        serialize_length(&mut buffer, self.keys.len() as u32);

        for key in &self.keys {
            let serialized_key = key.serialize();
            serialize_length(&mut buffer, serialized_key.len() as u32);
            buffer.extend_from_slice(&serialized_key);
        }

        match &self.node_type {
            NodeType::Leaf => {
                for value in &self.values {
                    match value {
                        Some(value) => {
                            buffer.extend_from_slice(&(value.offset as u64).to_le_bytes());
                        }
                        None => {
                            buffer.extend_from_slice(&0u64.to_le_bytes());
                        }
                    }
                }
            }
            NodeType::Internal => {
                for child in &self.children {
                    buffer.extend_from_slice(&(*child as u64).to_le_bytes());
                }
            }
        }

        buffer
    }

    pub fn deserialize(data: &[u8]) -> Self {
        let mut offset = 0;

        let node_type = deserialize_node_type(&data[offset..offset + 1]);
        offset += 1;
        let is_root = u8::from_le_bytes(data[offset..offset + 1].try_into().unwrap()) == 1;
        offset += 1;
        let parent_offset =
            u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;

        let keys_len = read_length(&data[offset..offset + 4]);
        offset += 4;

        let mut keys = Vec::new();
        for _ in 0..keys_len {
            let key_len = read_length(&data[offset..offset + 4]);
            offset += 4;
            let key = K::deserialize(&data[offset..offset + key_len]);
            offset += key_len;
            keys.push(key);
        }

        let mut values = Vec::new();
        let mut children = Vec::new();
        match node_type {
            NodeType::Leaf => {
                for _ in 0..keys_len {
                    let value_offset =
                        u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
                    offset += 8;
                    values.push(Some(NodeValue {
                        offset: value_offset,
                        value: None,
                    }));
                }
            }
            NodeType::Internal => {
                for _ in 0..keys_len {
                    let child_offset =
                        u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
                    offset += 8;
                    children.push(child_offset);
                }
            }
        }

        Node {
            keys,
            values,
            children,
            node_type,
            offset: 0,
            is_root,
            parent_offset: Some(parent_offset),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::ann_tree::serialization::{TreeDeserialization, TreeSerialization};

    #[derive(Debug, PartialEq, Clone, PartialOrd, Eq)]
    struct TestKey {
        key: String,
    }

    impl Ord for TestKey {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.key.cmp(&other.key)
        }
    }

    impl TreeSerialization for TestKey {
        fn serialize(&self) -> Vec<u8> {
            self.key.as_bytes().to_vec()
        }
    }

    impl TreeDeserialization for TestKey {
        fn deserialize(data: &[u8]) -> Self {
            TestKey {
                key: String::from_utf8(data.to_vec()).unwrap(),
            }
        }
    }

    #[derive(Debug, PartialEq, Clone)]
    struct TestValue {
        value: String,
    }

    impl TreeSerialization for TestValue {
        fn serialize(&self) -> Vec<u8> {
            self.value.as_bytes().to_vec()
        }
    }

    impl TreeDeserialization for TestValue {
        fn deserialize(data: &[u8]) -> Self {
            TestValue {
                value: String::from_utf8(data.to_vec()).unwrap(),
            }
        }
    }

    #[test]
    fn test_serialize_node_type() {
        assert_eq!(serialize_node_type(&NodeType::Leaf), [0]);
        assert_eq!(serialize_node_type(&NodeType::Internal), [1]);
    }

    #[test]
    fn test_deserialize_node_type() {
        assert_eq!(deserialize_node_type(&[0]), NodeType::Leaf);
        assert_eq!(deserialize_node_type(&[1]), NodeType::Internal);
    }

    #[test]
    fn test_serialize_length() {
        let mut buffer = Vec::new();
        serialize_length(&mut buffer, 10);
        assert_eq!(buffer, [10, 0, 0, 0]);
    }

    #[test]
    fn test_read_length() {
        assert_eq!(read_length(&[10, 0, 0, 0]), 10);
    }

    #[test]
    fn test_serialize_node() {
        let node = Node::<TestKey, TestValue> {
            keys: vec![TestKey {
                key: "key1".to_string(),
            }],
            values: vec![Some(NodeValue {
                offset: 0,
                value: Some(TestValue {
                    value: "value1".to_string(),
                }),
            })],
            children: vec![1],
            node_type: NodeType::Leaf,
            offset: 0,
            is_root: true,
            parent_offset: Some(0),
        };

        let serialized = node.serialize();
        let deserialized = Node::<TestKey, TestValue>::deserialize(&serialized);

        assert_eq!(node, deserialized);
    }

    #[test]
    fn test_serialize_internal_node() {
        let node = Node::<TestKey, TestValue> {
            keys: vec![TestKey {
                key: "key1".to_string(),
            }],
            values: vec![],
            children: vec![1],
            node_type: NodeType::Internal,
            offset: 0,
            is_root: true,
            parent_offset: Some(0),
        };

        let serialized = node.serialize();
        let deserialized = Node::<TestKey, TestValue>::deserialize(&serialized);

        assert_eq!(node, deserialized);
    }

    #[test]
    fn test_serialize_node_with_multiple_keys() {
        let node = Node::<TestKey, TestValue> {
            keys: vec![
                TestKey {
                    key: "key1".to_string(),
                },
                TestKey {
                    key: "key2".to_string(),
                },
            ],
            values: vec![
                Some(NodeValue {
                    offset: 0,
                    value: Some(TestValue {
                        value: "value1".to_string(),
                    }),
                }),
                Some(NodeValue {
                    offset: 1,
                    value: Some(TestValue {
                        value: "value2".to_string(),
                    }),
                }),
            ],
            children: vec![1, 2],
            node_type: NodeType::Leaf,
            offset: 0,
            is_root: true,
            parent_offset: Some(0),
        };

        let serialized = node.serialize();
        let deserialized = Node::<TestKey, TestValue>::deserialize(&serialized);

        assert_eq!(node, deserialized);
    }

    #[test]
    fn test_serialize_internal_node_with_multiple_keys() {
        let node = Node::<TestKey, TestValue> {
            keys: vec![
                TestKey {
                    key: "key1".to_string(),
                },
                TestKey {
                    key: "key2".to_string(),
                },
            ],
            values: vec![],
            children: vec![1, 2],
            node_type: NodeType::Internal,
            offset: 0,
            is_root: true,
            parent_offset: Some(0),
        };

        let serialized = node.serialize();
        let deserialized = Node::<TestKey, TestValue>::deserialize(&serialized);

        assert_eq!(node, deserialized);
    }

    #[test]
    fn test_serialize_node_with_no_keys() {
        let node = Node::<TestKey, TestValue> {
            keys: vec![],
            values: vec![],
            children: vec![],
            node_type: NodeType::Leaf,
            offset: 0,
            is_root: true,
            parent_offset: Some(0),
        };

        let serialized = node.serialize();
        let deserialized = Node::<TestKey, TestValue>::deserialize(&serialized);

        assert_eq!(node, deserialized);
    }

    #[test]
    fn test_serialize_internal_node_with_no_keys() {
        let node = Node::<TestKey, TestValue> {
            keys: vec![],
            values: vec![],
            children: vec![],
            node_type: NodeType::Internal,
            offset: 0,
            is_root: true,
            parent_offset: Some(0),
        };

        let serialized = node.serialize();
        let deserialized = Node::<TestKey, TestValue>::deserialize(&serialized);

        assert_eq!(node, deserialized);
    }
}
