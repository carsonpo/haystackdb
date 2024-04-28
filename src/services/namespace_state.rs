use crate::structures::dense_vector_list::DenseVectorList;
use crate::structures::inverted_index::InvertedIndex;
use crate::structures::metadata_index::MetadataIndex;
use crate::structures::wal::WAL;
use std::fs;
use std::io;
use std::path::PathBuf;

use super::LockService;

pub struct NamespaceState {
    pub namespace_id: String,
    pub metadata_index: MetadataIndex,
    pub inverted_index: InvertedIndex,
    pub vectors: DenseVectorList,
    pub wal: WAL,
    pub locks: LockService,
}

impl NamespaceState {
    pub fn new(path: PathBuf, namespace_id: String) -> io::Result<Self> {
        let metadata_path = path.clone().join("metadata.bin");
        let inverted_index_path = path.clone().join("inverted_index.bin");
        let wal_path = path.clone().join("wal");
        let locks_path = path.clone().join("locks");
        fs::create_dir_all(&wal_path).expect("Failed to create directory");
        fs::create_dir_all(&locks_path).expect("Failed to create directory");
        let vectors_path = path.clone().join("vectors.bin");

        let metadata_index = MetadataIndex::new(metadata_path);
        let inverted_index = InvertedIndex::new(inverted_index_path);
        let wal = WAL::new(wal_path, namespace_id.clone())?;
        let vectors = DenseVectorList::new(vectors_path, 100_000)?;
        let locks = LockService::new(locks_path);

        Ok(NamespaceState {
            namespace_id,
            metadata_index,
            inverted_index,
            vectors,
            wal,
            locks,
        })
    }

    pub fn reconstruct_from_timestamp(self, _timestamp: u64) -> io::Result<Self> {
        unimplemented!()
    }
}
