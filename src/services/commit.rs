use crate::constants::VECTOR_SIZE;
// use crate::structures::inverted_index::InvertedIndexItem;
// use crate::structures::metadata_index::{KVPair, MetadataIndexItem};
use crate::structures::filters::{KVPair, KVValue};
use crate::utils::compress_string;
use rusqlite::Result;

use super::namespace_state::NamespaceState;
use std::collections::HashMap;

use std::io;
use std::os::unix::fs as unix_fs;
use std::path::PathBuf;

pub struct CommitService {
    pub state: NamespaceState,
}

impl CommitService {
    pub fn new(path: PathBuf, namespace_id: String) -> io::Result<Self> {
        let state = NamespaceState::new(path, namespace_id)?;
        Ok(CommitService { state })
    }

    pub fn commit(&mut self) -> Result<()> {
        let commits = self.state.wal.get_uncommitted(100000)?;

        let mut processed = 0;

        println!("Commits to process: {:?}", commits.len());

        // let mut vectors = Vec::new();
        // let mut kvs = Vec::new();
        // let mut ids = Vec::new();

        // for commit in commits.iter() {
        //     let inner_vectors = commit.vectors.clone();
        //     let inner_kvs = commit.kvs.clone();
        //     let inner_ids: Vec<u128> = inner_vectors
        //         .iter()
        //         .map(|_| uuid::Uuid::new_v4().as_u128())
        //         .collect();

        //     for ((vector, kv), id) in inner_vectors
        //         .iter()
        //         .zip(inner_kvs.iter())
        //         .zip(inner_ids.iter())
        //     {
        //         vectors.push(vector.clone());
        //         kvs.push(kv.clone());
        //         ids.push(id.clone());
        //     }
        // }

        // self.state.vectors.bulk_insert(vectors, ids, kvs);

        for commit in commits {
            let vectors = commit.vectors;
            let kvs: Vec<Vec<_>> = commit
                .kvs
                .clone()
                .iter()
                .map(|kv| {
                    kv.clone()
                        .iter()
                        .filter(|item| item.key != "text")
                        .cloned()
                        .collect()
                })
                .collect::<Vec<_>>();

            let texts: Vec<KVValue> = commit
                .kvs
                .clone()
                .iter()
                .map(|kv| {
                    kv.clone()
                        .iter()
                        .filter(|item| item.key == "text")
                        .collect::<Vec<_>>()
                        .first()
                        .unwrap_or(&&KVPair {
                            key: "text".to_string(),
                            value: KVValue::String("".to_string()),
                        })
                        .value
                        .clone()
                })
                .collect::<Vec<_>>();

            println!("Processing commit: {:?}", processed);

            processed += 1;

            for ((vector, kv), texts) in vectors.iter().zip(kvs).zip(texts) {
                let id = uuid::Uuid::new_v4().as_u128();

                self.state.vectors.insert(*vector, id, kv);
                // self.state.texts.insert(id, texts.clone());
                match texts {
                    KVValue::String(text) => {
                        self.state
                            .texts
                            .insert(id, compress_string(&text))
                            .expect("Failed to insert text");
                    }
                    _ => {}
                }
            }

            // self.state.wal.mark_commit_finished(commit.hash)?;
        }

        self.state
            .vectors
            .true_calibrate()
            .expect("Failed to calibrate");

        Ok(())
    }

    pub fn recover_point_in_time(&mut self, timestamp: u64) -> Result<()> {
        println!("Recovering to timestamp: {}", timestamp);
        let versions: Vec<i32> = self
            .state
            .get_all_versions()
            .expect("Failed to get versions");
        let max_version = versions.iter().max().unwrap();
        let new_version = max_version + 1;

        println!("Versions: {:?}", versions);

        println!("Creating new version: {}", new_version);

        let new_version_path = self
            .state
            .path
            .parent()
            .unwrap()
            .join(format!("v{}", new_version));

        let mut fresh_state =
            NamespaceState::new(new_version_path.clone(), self.state.namespace_id.clone())
                .expect("Failed to create fresh state");

        let commits = self.state.wal.get_commits_before(timestamp)?;
        let commits_len = commits.len();

        if commits.len() == 0 {
            return Ok(());
        }

        println!("Commits to PITR: {:?}", commits_len);

        let mut processed = 0;

        // fresh_state.wal.mark_commit_finished(commit.hash)?;

        for commit in commits {
            let vectors = commit.vectors;
            let kvs = commit.kvs;

            for (vector, kv) in vectors.iter().zip(kvs.iter()) {
                let id = uuid::Uuid::new_v4().as_u128();

                fresh_state.vectors.insert(vector.clone(), id, kv.clone());
            }

            fresh_state.wal.mark_commit_finished(commit.hash)?;

            processed += 1;

            if processed % 1000 == 0 {
                println!("Processed: {}/{}", processed, commits_len);
            }
        }

        // update symlink for /current
        let current_path = self.state.path.clone();

        println!("Removing current symlink: {:?}", current_path);

        std::fs::remove_file(&current_path).expect("Failed to remove current symlink");
        unix_fs::symlink(&new_version_path, &current_path).expect("Failed to create symlink");

        Ok(())
    }

    pub fn calibrate(&mut self) {
        self.state
            .vectors
            .true_calibrate()
            .expect("Failed to calibrate");
    }

    pub fn add_to_wal(
        &mut self,
        vectors: Vec<[f32; VECTOR_SIZE]>,
        kvs: Vec<Vec<KVPair>>,
    ) -> Result<()> {
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
    ) -> Result<()> {
        self.state.wal.batch_add_to_wal(vectors, kvs)?;

        Ok(())
    }
}
