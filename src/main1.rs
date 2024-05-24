extern crate haystackdb;
use haystackdb::constants::VECTOR_SIZE;
use haystackdb::services::commit::CommitService;
use haystackdb::services::query::QueryService;
use haystackdb::structures::filters::{Filter, KVPair, KVValue};
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use uuid;

fn random_vec() -> [f32; VECTOR_SIZE] {
    let mut vec = [0.0; VECTOR_SIZE];
    for i in 0..VECTOR_SIZE {
        vec[i] = rand::random::<f32>() * 2.0 - 1.0;
    }
    vec
}

fn main() {
    let namespace_id = uuid::Uuid::new_v4().to_string();
    let path = PathBuf::from_str("tests/data")
        .expect("Failed to create path")
        .join("namespaces")
        .join(namespace_id.clone());
    fs::create_dir_all(&path).expect("Failed to create directory");
    let mut commit_service = CommitService::new(path.clone(), namespace_id.clone())
        .expect("Failed to create commit service");

    let start = std::time::Instant::now();
    // for _ in 0..20000 {
    //     commit_service
    //         .add_to_wal(
    //             vec![random_vec()],
    //             vec![vec![KVPair {
    //                 key: "key".to_string(),
    //                 value: "value".to_string(),
    //             }]],
    //         )
    //         .expect("Failed to add to WAL");
    // }

    const NUM_VECTORS: usize = 10_000;

    let batch_vectors: Vec<Vec<[f32; VECTOR_SIZE]>> =
        (0..NUM_VECTORS).map(|_| vec![random_vec()]).collect();
    let batch_kvs: Vec<Vec<Vec<KVPair>>> = (0..NUM_VECTORS)
        .map(|_| {
            vec![vec![KVPair {
                key: "key".to_string(),
                value: KVValue::String("value".to_string()),
            }]]
        })
        .collect();

    println!("Batch creation took: {:?}", start.elapsed());
    commit_service
        .batch_add_to_wal(batch_vectors, batch_kvs)
        .expect("Failed to add to WAL");

    println!("Add to WAL took: {:?}", start.elapsed());

    // commit_service
    //     .add_to_wal(
    //         vec![[0.0; VECTOR_SIZE]],
    //         vec![vec![KVPair {
    //             key: "key".to_string(),
    //             value: "value".to_string(),
    //         }]],
    //     )
    //     .expect("Failed to add to WAL");

    let start = std::time::Instant::now();

    commit_service.commit().expect("Failed to commit");

    println!("Commit took: {:?}", start.elapsed());

    commit_service.calibrate();

    commit_service.state.vectors.summarize_tree();

    let mut query_service =
        QueryService::new(path.clone(), namespace_id).expect("Failed to create query service");

    let _start = std::time::Instant::now();

    const NUM_RUNS: usize = 100;

    let start = std::time::Instant::now();

    for _ in 0..NUM_RUNS {
        let result = query_service
            .query(
                &random_vec(),
                &Filter::Eq("key".to_string(), "value".to_string()),
                1,
            )
            .expect("Failed to query");

        // println!("{:?}", result);
        if result.len() == 0 {
            println!("No results found");
        }
    }

    println!("Query took: {:?}", start.elapsed().div_f32(NUM_RUNS as f32));

    // let result = query_service
    //     .query(
    //         &[0.0; VECTOR_SIZE],
    //         vec![KVPair {
    //             key: "key".to_string(),
    //             value: "value".to_string(),
    //         }],
    //         1,
    //     )
    //     .expect("Failed to query");

    // println!("{:?}", result);

    // println!("Query took: {:?}", start.elapsed());
}

// fn main() {
//     let mut storage_manager: StorageManager<i32, String> = StorageManager::new(
//         PathBuf::from_str("tests/data/test.db").expect("Failed to create path"),
//     )
//     .expect("Failed to create storage manager");

//     let mut node: Node<i32, String> = Node::new_leaf(0);

//     for i in 0..2048 {
//         node.set_key_value(i, uuid::Uuid::new_v4().to_string());
//     }

//     let serialized = Node::serialize(&node);
//     let deserialized = Node::deserialize(&serialized);

//     assert_eq!(node, deserialized);

//     let offset = storage_manager
//         .store_node(&mut node)
//         .expect("Failed to store node");

//     node.offset = offset;

//     let mut loaded_node = storage_manager
//         .load_node(offset)
//         .expect("Failed to load node");

//     loaded_node.offset = offset;

//     assert_eq!(loaded_node, node);
// }
