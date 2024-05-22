use crate::structures::ann_tree::ANNTree;
// use crate::structures::dense_vector_list::DenseVectorList;
// use crate::structures::inverted_index::InvertedIndex;
// use crate::structures::metadata_index::MetadataIndex;
use crate::structures::mmap_tree::Tree;
use crate::structures::wal::WAL;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use super::LockService;

pub struct NamespaceState {
    pub namespace_id: String,
    // pub metadata_index: MetadataIndex,
    // pub inverted_index: InvertedIndex,
    pub texts: Tree<u128, Vec<u8>>,
    pub vectors: ANNTree,
    pub wal: WAL,
    pub locks: LockService,
    pub path: PathBuf,
}

fn get_all_versions(path: &Path) -> io::Result<Vec<i32>> {
    let mut versions = Vec::new();
    for entry in fs::read_dir(&path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let version = path.file_name().unwrap().to_str().unwrap().to_string();
            // parse as int without `v` to see if it's a version
            // must remove the v first
            if version.starts_with("v") {
                let version = version[1..].parse::<i32>();
                if version.is_ok() {
                    versions.push(version.unwrap());
                }
            }
        }
    }
    Ok(versions)
}

impl NamespaceState {
    pub fn new(path: PathBuf, namespace_id: String) -> io::Result<Self> {
        // path should be .../current, which should be a symlink to the current version

        // println!("Creating namespace state with path: {:?}", path);

        if !path.exists() {
            fs::create_dir_all(&path.clone().parent().unwrap())
                .expect("Failed to create directory");
        }

        let versions = get_all_versions(path.clone().parent().unwrap())?;

        if versions.len() == 0 {
            // create v0
            // println!("Creating v0");
            let version_path = path.clone().parent().unwrap().join("v0");
            // println!("Creating version path: {:?}", version_path);
            fs::create_dir_all(&version_path).expect("Failed to create directory");

            // create symlink

            std::os::unix::fs::symlink(&version_path, &path).expect("Failed to create symlink");
        }

        let metadata_path = path.clone().join("metadata.bin");
        let inverted_index_path = path.clone().join("inverted_index.bin");
        let wal_path = path.clone().join("wal");
        let locks_path = path.clone().join("locks");

        fs::create_dir_all(&wal_path).unwrap_or_default();

        fs::create_dir_all(&locks_path).unwrap_or_default();

        let vectors_path = path.clone().join("vectors.bin");
        let texts_path = path.clone().join("texts.bin");

        // let metadata_index = MetadataIndex::new(metadata_path);
        // let inverted_index = InvertedIndex::new(inverted_index_path);
        let wal = WAL::new(wal_path, namespace_id.clone()).expect("Failed to create WAL");
        let vectors = ANNTree::new(vectors_path)?;
        let locks = LockService::new(locks_path);
        let texts = Tree::new(texts_path)?;

        Ok(NamespaceState {
            namespace_id,
            // metadata_index,
            // inverted_index,
            texts,
            vectors,
            wal,
            locks,
            path,
        })
    }

    pub fn get_all_versions(&self) -> io::Result<Vec<i32>> {
        let mut versions = Vec::new();
        for entry in fs::read_dir(&self.path.parent().unwrap())? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                // println!("path: {:?}", path);
                let version = path.file_name().unwrap().to_str().unwrap().to_string();
                // println!("version: {:?}", version);
                // parse as int without `v` to see if it's a version
                // must remove the v first
                if version.starts_with("v") {
                    let version = version[1..].parse::<i32>();
                    if version.is_ok() {
                        versions.push(version.unwrap());
                    }
                }
            }
        }
        // println!("versions: {:?}", versions);
        Ok(versions)
    }
}
