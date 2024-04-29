extern crate haystackdb;

use haystackdb::structures::filters::{Filter, Filters};
use haystackdb::structures::inverted_index::{
    compress_indices, decompress_indices, InvertedIndex, InvertedIndexItem,
};
use haystackdb::structures::metadata_index::KVPair;
use serde::{Deserialize, Serialize};
use serde_json;
use std::fs;
use std::path::PathBuf;
use uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Query {
    filters: Filter,
}

fn handle_query(json_query: &str, index: &mut InvertedIndex) -> Filters {
    let query: Query = serde_json::from_str(json_query).expect("Failed to parse query");
    Filters::evaluate(&query.filters, index)
}

#[cfg(test)]
mod math_tests {

    use super::*;

    fn setup_inverted_index() -> InvertedIndex {
        let path = PathBuf::from("tests/data")
            .join(uuid::Uuid::new_v4().to_string())
            .join("inverted_index.bin");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut index = InvertedIndex::new(path);
        // Insert some test data
        index.insert(
            KVPair::new("page_id".to_string(), "page1".to_string()),
            InvertedIndexItem {
                indices: vec![1, 2],
                ids: vec![0, 0],
            },
        );
        index.insert(
            KVPair::new("page_id".to_string(), "page2".to_string()),
            InvertedIndexItem {
                indices: vec![3],
                ids: vec![0],
            },
        );
        index.insert(
            KVPair::new("public".to_string(), "1".to_string()),
            InvertedIndexItem {
                indices: vec![1, 3],
                ids: vec![0, 0],
            },
        );
        index.insert(
            KVPair::new("permission_id".to_string(), "3iQK2VC4".to_string()),
            InvertedIndexItem {
                indices: vec![2],
                ids: vec![0],
            },
        );
        index.insert(
            KVPair::new("permission_id".to_string(), "wzw8zpnQ".to_string()),
            InvertedIndexItem {
                indices: vec![3],
                ids: vec![0],
            },
        );
        index
    }

    #[test]
    fn test_compression_decompression() {
        let indices = vec![1, 2, 3, 6, 7, 8];
        let compressed = compress_indices(indices.clone());
        let decompressed = decompress_indices(compressed);

        assert_eq!(
            indices, decompressed,
            "Decompression did not match the original indices"
        );
    }

    #[test]
    fn test_basic_and_query() {
        let mut index = setup_inverted_index();
        let json_query = r#"{"filters":{"type":"Eq","args":["public","1"]}}"#;
        let result = handle_query(json_query, &mut index);
        assert_eq!(result.get_indices(), vec![1, 3]); // Expected indices
    }

    #[test]
    fn test_empty_query() {
        let mut index = setup_inverted_index();
        let json_query = r#"{"filters":{"type":"And","args":[]}}"#;
        let result = handle_query(json_query, &mut index);
        assert!(result.get_indices().is_empty()); // Should handle empty AND gracefully
    }

    #[test]
    fn test_nonexistent_key() {
        let mut index = setup_inverted_index();
        let json_query = r#"{"filters":{"type":"Eq","args":["nonexistent","value"]}}"#;
        let result = handle_query(json_query, &mut index);
        assert!(result.get_indices().is_empty()); // No crash, just empty result
    }

    #[test]
    fn test_single_eq() {
        let mut index = setup_inverted_index();
        let json_query = r#"{"filters":{"type":"Eq","args":["public","1"]}}"#;
        let result = handle_query(json_query, &mut index);
        assert_eq!(result.get_indices(), vec![1, 3]);
    }

    #[test]
    fn test_single_in() {
        let mut index = setup_inverted_index();
        let json_query = r#"{"filters":{"type":"In","args":["page_id",["page1","page2"]]}}"#;
        let result = handle_query(json_query, &mut index);
        assert_eq!(result.get_indices(), vec![1, 2, 3]);
    }

    #[test]
    fn test_combined_and() {
        let mut index = setup_inverted_index();
        let json_query = r#"
        {
            "filters": {
                "type": "And",
                "args": [
                    {
                        "type": "In",
                        "args": ["page_id", ["page1"]]
                    },
                    {
                        "type": "Eq",
                        "args": ["public", "1"]
                    }
                ]
            }
        }
        "#;
        let result = handle_query(json_query, &mut index);
        assert_eq!(result.get_indices(), vec![1]);
    }

    #[test]
    fn test_complex_or() {
        let mut index = setup_inverted_index();
        let json_query = r#"{"filters":{"type":"Or","args":[{"type":"Eq","args":["public","1"]},{"type":"In","args":["permission_id",["wzw8zpnQ"]]}]}}"#;
        let result = handle_query(json_query, &mut index);
        assert_eq!(result.get_indices(), vec![1, 3]); // Should be the union of [1, 3] and [3]
    }
}
