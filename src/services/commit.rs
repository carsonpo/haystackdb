use crate::constants::VECTOR_SIZE;
use crate::structures::inverted_index::InvertedIndexItem;
use crate::structures::metadata_index::{KVPair, MetadataIndexItem};

use super::namespace_state::NamespaceState;
use ahash::AHashMap;

use std::io;
use std::path::PathBuf;

pub struct CommitService {
    pub state: NamespaceState,
}

impl CommitService {
    pub fn new(path: PathBuf, namespace_id: String) -> io::Result<Self> {
        let state = NamespaceState::new(path, namespace_id)?;
        Ok(CommitService { state })
    }

    pub fn commit(&mut self) -> io::Result<()> {
        let commits = self.state.wal.get_uncommitted(100000)?;

        let commits_len = commits.len();

        println!("Commits: {:?}", commits_len);

        if commits.len() == 0 {
            return Ok(());
        }

        let mut processed = 0;

        let merged_commits = commits
            .iter()
            .fold((Vec::new(), Vec::new()), |mut items, commit| {
                let vectors = commit.vectors.clone();
                let kvs = commit.kvs.clone();

                items.0.extend(vectors);
                items.1.extend(kvs);

                items
            });

        for (vectors, kvs) in vec![merged_commits] {
            // let vectors = commit.vectors;
            // let kvs = commit.kvs;

            if vectors.len() != kvs.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Quantized vectors length mismatch",
                ));
            }

            println!(
                "Processing commit: {} of {} with vectors of len: {}",
                processed,
                commits_len,
                vectors.len()
            );

            processed += 1;

            // generate u128 ids

            let ids = (0..vectors.len())
                .map(|_| uuid::Uuid::new_v4().as_u128())
                .collect::<Vec<u128>>();

            println!("Generated ids");

            let vector_indices = self.state.vectors.batch_push(vectors)?;

            println!("Pushed vectors");

            let mut inverted_index_items: AHashMap<KVPair, Vec<(usize, u128)>> = AHashMap::new();

            // let mut metadata_index_items = Vec::new();

            for (idx, kv) in kvs.iter().enumerate() {
                let metadata_index_item = MetadataIndexItem {
                    id: ids[idx],
                    kvs: kv.clone(),
                    vector_index: vector_indices[idx],
                    // namespaced_id: self.state.namespace_id.clone(),
                };

                println!("Inserting id: {},  {} of {}", ids[idx], idx, ids.len());

                // metadata_index_items.push((ids[idx], metadata_index_item));

                self.state
                    .metadata_index
                    .insert(ids[idx], metadata_index_item);

                for kv in kv {
                    // let inverted_index_item = InvertedIndexItem {
                    //     indices: vec![vector_indices[idx]],
                    //     ids: vec![ids[idx]],
                    // };

                    // self.state
                    //     .inverted_index
                    //     .insert_append(kv.clone(), inverted_index_item);

                    inverted_index_items
                        .entry(kv.clone())
                        .or_insert_with(Vec::new)
                        .push((vector_indices[idx], ids[idx]));
                }
            }

            // self.state.metadata_index.batch_insert(metadata_index_items);

            for (kv, items) in inverted_index_items {
                let inverted_index_item = InvertedIndexItem {
                    indices: items.iter().map(|(idx, _)| *idx).collect(),
                    ids: items.iter().map(|(_, id)| *id).collect(),
                };

                self.state
                    .inverted_index
                    .insert_append(kv, inverted_index_item);
            }
        }

        for commit in commits {
            self.state.wal.mark_commit_finished(commit.hash)?;
        }

        Ok(())
    }

    pub fn rollback(&mut self, _timestamp: u64) -> io::Result<()> {
        unimplemented!()
    }

    pub fn run(&mut self) -> io::Result<()> {
        unimplemented!()
    }

    pub fn add_to_wal(
        &mut self,
        vectors: Vec<[f32; VECTOR_SIZE]>,
        kvs: Vec<Vec<KVPair>>,
    ) -> io::Result<()> {
        if vectors.len() != vectors.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Quantized vectors length mismatch",
            ));
        }

        // self.state.wal.commit(hash, quantized_vectors, kvs)
        self.state
            .wal
            .add_to_wal(vectors, kvs)
            .expect("Failed to add to wal");

        Ok(())
    }

    pub fn batch_add_to_wal(
        &mut self,
        vectors: Vec<Vec<[f32; VECTOR_SIZE]>>,
        kvs: Vec<Vec<Vec<KVPair>>>,
    ) -> io::Result<()> {
        if vectors.len() != kvs.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Quantized vectors length mismatch",
            ));
        }

        self.state.wal.batch_add_to_wal(vectors, kvs)?;

        Ok(())
    }
}
