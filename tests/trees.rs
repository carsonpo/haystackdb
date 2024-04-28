extern crate haystackdb;

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::str::FromStr;

    use haystackdb::structures::mmap_tree::node::{Node, NodeType};
    use haystackdb::structures::mmap_tree::storage::{StorageManager, HEADER_SIZE};
    use haystackdb::structures::mmap_tree::Tree;
    use std::fs;
    use uuid;

    #[test]
    fn test_store_and_load_node() {
        let path = PathBuf::from_str("tests/data")
            .unwrap()
            .join(uuid::Uuid::new_v4().to_string())
            .join("test.bin");
        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");

        let mut storage_manager: StorageManager<i32, String> =
            StorageManager::new(path.clone()).unwrap();

        let mut node = Node::new_leaf(0);
        node.keys.push(1);
        node.values.push(Some("one".to_string()));

        // Store the node
        let offset = storage_manager.store_node(&mut node).unwrap();
        assert_eq!(offset, HEADER_SIZE); // Check that the node is stored at the correct offset

        // Load the node
        let loaded_node = storage_manager.load_node(offset).unwrap();
        assert_eq!(loaded_node.keys, vec![1]);
        assert_eq!(loaded_node.values, vec![Some("one".to_string())]);
    }

    #[test]
    fn test_store_multiple_nodes() {
        let path = PathBuf::from_str("tests/data")
            .unwrap()
            .join(uuid::Uuid::new_v4().to_string())
            .join("test.bin");
        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");

        let mut storage_manager: StorageManager<i32, String> =
            StorageManager::new(path.clone()).unwrap();

        let mut node1 = Node::new_leaf(0);
        node1.keys.push(1);
        node1.values.push(Some("one".to_string()));

        let mut node2 = Node::new_leaf(0);
        node2.keys.push(2);
        node2.values.push(Some("two".to_string()));

        // Store the first node
        let offset1 = storage_manager.store_node(&mut node1).unwrap();
        assert_eq!(offset1, HEADER_SIZE);

        // Store the second node
        let offset2 = storage_manager.store_node(&mut node2).unwrap();
        assert!(offset2 > offset1); // Ensure that the second node is stored after the first

        // Load the first node
        let loaded_node1 = storage_manager.load_node(offset1).unwrap();
        assert_eq!(loaded_node1.keys, vec![1]);
        assert_eq!(loaded_node1.values, vec![Some("one".to_string())]);

        // Load the second node
        let loaded_node2 = storage_manager.load_node(offset2).unwrap();
        assert_eq!(loaded_node2.keys, vec![2]);
        assert_eq!(loaded_node2.values, vec![Some("two".to_string())]);
    }

    // #[test]
    // fn test_resize_storage() {
    //     let path = PathBuf::from_str("tests/data")
    //         .unwrap()
    //         .join(uuid::Uuid::new_v4().to_string())
    //     let mut storage_manager: StorageManager<i32, String> =
    //         StorageManager::new(path.clone()).unwrap();

    //     let mut large_node = Node::new_leaf(0);
    //     for i in 0..1000 {
    //         large_node.keys.push(i);
    //         large_node.values.push(Some(format!("value_{}", i)));
    //     }

    //     // Store the large node
    //     let offset = storage_manager.store_node(&mut large_node).unwrap();
    //     assert_eq!(offset, HEADER_SIZE);

    //     // Load the large node
    //     let loaded_node = storage_manager.load_node(offset).unwrap();
    //     assert_eq!(loaded_node.keys.len(), 1000);
    //     assert_eq!(loaded_node.values.len(), 1000);
    // }

    #[test]
    fn test_new_leaf() {
        let node: Node<i32, String> = Node::new_leaf(0);
        assert!(node.keys.is_empty());
        assert!(node.values.is_empty());
        assert!(node.children.is_empty());
        assert_eq!(node.node_type, NodeType::Leaf);
    }

    #[test]
    fn test_search_in_leaf() {
        let path = PathBuf::from_str("tests/data")
            .unwrap()
            .join(uuid::Uuid::new_v4().to_string())
            .join("test.bin");
        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");

        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");

        let mut tree = Tree::new(path).expect("Failed to create tree");
        tree.insert(1, "one".to_string()).unwrap();
        tree.insert(2, "two".to_string()).unwrap();
        assert_eq!(tree.search(1).unwrap(), Some("one".to_string()));
        assert_eq!(tree.search(2).unwrap(), Some("two".to_string()));
        assert_eq!(tree.search(3).unwrap(), None);
    }

    #[test]
    fn test_complex_tree_operations() {
        let path = PathBuf::from_str("tests/data")
            .unwrap()
            .join(uuid::Uuid::new_v4().to_string())
            .join("test.bin");
        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");

        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");

        let mut tree = Tree::new(path).expect("Failed to create tree");
        for i in 0..10 {
            tree.insert(i, format!("value_{}", i)).unwrap();
        }
        assert_eq!(tree.search(5).unwrap(), Some("value_5".to_string()));
        assert_eq!(tree.search(9).unwrap(), Some("value_9".to_string()));
        assert_eq!(tree.search(10).unwrap(), None);
    }

    #[test]
    fn test_serialization_and_deserialization() {
        let mut node: Node<i32, String> = Node::new_leaf(0);
        node.set_key_value(0, "value_0".to_string());
        node.set_key_value(1, "value_1".to_string());
        let serialized = node.serialize();
        let deserialized: Node<i32, String> = Node::deserialize(&serialized);

        assert_eq!(node.keys, deserialized.keys);
        assert_eq!(node.values, deserialized.values);
        assert_eq!(node.children, deserialized.children);
    }

    #[test]
    fn test_tree_initialization() {
        let path = PathBuf::from_str("tests/data")
            .unwrap()
            .join(uuid::Uuid::new_v4().to_string())
            .join("test.bin");
        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");

        let tree: Result<Tree<i32, String>, std::io::Error> = Tree::new(path);
        assert!(tree.is_ok());
    }

    #[test]
    fn test_insert_search_leaf() {
        let path = PathBuf::from_str("tests/data")
            .unwrap()
            .join(uuid::Uuid::new_v4().to_string())
            .join("test.bin");
        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");

        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");
        let mut tree = Tree::new(path).expect("Failed to create tree");

        tree.insert(1, "one".to_string()).unwrap();
        tree.insert(2, "two".to_string()).unwrap();

        assert_eq!(tree.search(1).unwrap(), Some("one".to_string()));
        assert_eq!(tree.search(2).unwrap(), Some("two".to_string()));
        assert_eq!(tree.search(3).unwrap(), None);
    }

    // Edge Cases

    #[test]
    fn test_insert_duplicate_keys() {
        let path = PathBuf::from_str("tests/data")
            .unwrap()
            .join(uuid::Uuid::new_v4().to_string())
            .join("test.bin");
        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");

        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");
        let mut tree = Tree::new(path).expect("Failed to create tree");

        tree.insert(1, "one".to_string()).unwrap();
        tree.insert(1, "one_duplicate".to_string()).unwrap(); // Assuming overwrite behavior

        assert_eq!(tree.search(1).unwrap(), Some("one_duplicate".to_string()));
    }

    #[test]
    fn test_search_non_existent_key() {
        let path = PathBuf::from_str("tests/data")
            .unwrap()
            .join(uuid::Uuid::new_v4().to_string())
            .join("test.bin");
        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");

        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");
        let mut tree: Tree<i32, String> = Tree::new(path).expect("Failed to create tree");

        assert_eq!(tree.search(999).unwrap(), None);
    }
    // Complex Operations

    #[test]
    fn test_complex_insertions() {
        let path = PathBuf::from_str("tests/data")
            .unwrap()
            .join(uuid::Uuid::new_v4().to_string())
            .join("test.bin");
        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");
        let mut tree = Tree::new(path).expect("Failed to create tree");

        for i in 0..100 {
            tree.insert(i, format!("value_{}", i))
                .expect(format!("Failed to insert {}", i).as_str());
        }

        for i in 0..100 {
            assert_eq!(tree.search(i).unwrap(), Some(format!("value_{}", i)));
        }
    }

    #[test]
    fn test_large_scale_insert_search() {
        let path = PathBuf::from_str("tests/data")
            .unwrap()
            .join(uuid::Uuid::new_v4().to_string())
            .join("test.bin");
        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");

        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");
        let mut tree = Tree::new(path).unwrap();

        let num_items = 1000;
        for i in 0..num_items {
            tree.insert(i, format!("value_{}", i))
                .expect(format!("Failed to insert {}", i).as_str());
        }

        for i in 0..num_items {
            assert_eq!(tree.search(i).unwrap(), Some(format!("value_{}", i)));
        }
    }

    #[test]
    fn test_repeated_insertions_same_key() {
        let path = PathBuf::from_str("tests/data")
            .unwrap()
            .join(uuid::Uuid::new_v4().to_string())
            .join("test.bin");
        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");

        let mut tree = Tree::new(path).unwrap();

        tree.insert(1, "one".to_string()).unwrap();
        tree.insert(1, "still_one".to_string()).unwrap(); // Try inserting the same key

        // Check that the value has not been replaced if replacing isn't supported
        assert_eq!(tree.search(1).unwrap(), Some("still_one".to_string()));
    }

    // #[test]
    // fn test_insertion_order_independence() {
    //     let path = PathBuf::from_str("tests/data")
    //         .unwrap()
    //         .join(uuid::Uuid::new_v4().to_string());
    //     let mut tree = Tree::new(path.clone()).unwrap();
    //     let mut tree_reverse = Tree::new(path).unwrap();

    //     let keys = vec![3, 1, 4, 1, 5, 9, 2];
    //     let values = vec!["three", "one", "four", "one", "five", "nine", "two"];

    //     for (&k, &v) in keys.iter().zip(values.iter()) {
    //         tree.insert(k, v.to_string()).unwrap();
    //     }

    //     for (&k, &v) in keys.iter().zip(values.iter()).rev() {
    //         tree_reverse.insert(k, v.to_string()).unwrap();
    //     }

    //     for &k in &keys {
    //         assert_eq!(tree.search(k).unwrap(), tree_reverse.search(k).unwrap());
    //     }
    // }

    #[test]
    fn test_search_non_existent_keys() {
        let path = PathBuf::from_str("tests/data")
            .unwrap()
            .join(uuid::Uuid::new_v4().to_string())
            .join("test.bin");
        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");

        let mut tree: Tree<i32, String> = Tree::new(path).unwrap();

        assert_eq!(tree.search(999).unwrap(), None);
    }

    #[test]
    fn test_insert_search_edge_integers() {
        let path = PathBuf::from_str("tests/data")
            .unwrap()
            .join(uuid::Uuid::new_v4().to_string())
            .join("test.bin");
        fs::create_dir_all(&path.parent().unwrap()).expect("Failed to create directory");

        let mut tree = Tree::new(path).unwrap();

        let min_int = i32::MIN;
        let max_int = i32::MAX;

        tree.insert(min_int, "minimum".to_string()).unwrap();
        tree.insert(max_int, "maximum".to_string()).unwrap();

        assert_eq!(tree.search(min_int).unwrap(), Some("minimum".to_string()));
        assert_eq!(tree.search(max_int).unwrap(), Some("maximum".to_string()));
    }
}
