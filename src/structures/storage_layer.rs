use crate::services::LockService;
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::io::{Read, Write};
use std::path::PathBuf;

pub struct StorageLayer {
    path: PathBuf,
    root_offset_path: PathBuf,
    used_blocks_path: PathBuf,
}

pub const SIZE_OF_U64: usize = std::mem::size_of::<u64>();
pub const HEADER_SIZE: usize = SIZE_OF_U64 * 2; // Used space + root offset

pub const BLOCK_SIZE: usize = 4096; // typical page size
pub const BLOCK_HEADER_SIZE: usize = SIZE_OF_U64 * 5 + 1; // Index in chain + Primary index + Next block offset + Previous block offset + Serialized node length + Is primary
pub const BLOCK_DATA_SIZE: usize = BLOCK_SIZE - BLOCK_HEADER_SIZE;

impl StorageLayer {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let root_offset_path = path.join("root_offset.bin");
        let used_blocks_path = path.join("used_blocks.bin");

        fs::create_dir_all(&path).expect("Failed to create directory");

        let mut root_offset_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(root_offset_path.clone())
            .expect("Failed to create root offset file");
        root_offset_file
            .write_all(&[0u8; SIZE_OF_U64])
            .expect("Failed to write to root offset file");
        root_offset_file
            .sync_all()
            .expect("Failed to sync root offset file");

        let mut used_blocks_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(used_blocks_path.clone())
            .expect("Failed to create used blocks file");

        used_blocks_file
            .write_all(&[0u8; SIZE_OF_U64])
            .expect("Failed to write to used blocks file");
        used_blocks_file
            .sync_all()
            .expect("Failed to sync used blocks file");

        Ok(StorageLayer {
            path,
            root_offset_path,
            used_blocks_path,
        })
    }

    pub fn used_blocks(&self) -> usize {
        // (u64::from_le_bytes(self.mmap[0..SIZE_OF_U64].try_into().unwrap()) as usize) + 1

        let mut file = OpenOptions::new()
            .read(true)
            .open(self.used_blocks_path.clone())
            .expect("Failed to open used blocks file");
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .expect("Failed to read used blocks file");
        u64::from_le_bytes(bytes.try_into().unwrap()) as usize + 1usize
    }

    pub fn set_used_blocks(&mut self, used_blocks: usize) {
        // self.mmap[0..SIZE_OF_U64].copy_from_slice(&(used_blocks as u64).to_le_bytes());

        let mut file = OpenOptions::new()
            .write(true)
            .open(self.used_blocks_path.clone())
            .expect("Failed to open used blocks file");
        file.write_all(&(used_blocks as u64).to_le_bytes());
        // file.sync_all().expect("Failed to sync used blocks file");
    }

    pub fn root_offset(&self) -> usize {
        // u64::from_le_bytes(
        //     self.mmap[SIZE_OF_U64..(2 * SIZE_OF_U64)]
        //         .try_into()
        //         .unwrap(),
        // ) as usize

        let mut file = OpenOptions::new()
            .read(true)
            .open(self.root_offset_path.clone())
            .expect("Failed to open root offset file");

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .expect("Failed to read root offset file");

        u64::from_le_bytes(bytes.try_into().unwrap()) as usize
    }

    pub fn set_root_offset(&mut self, root_offset: usize) {
        // self.mmap[SIZE_OF_U64..(2 * SIZE_OF_U64)]
        //     .copy_from_slice(&(root_offset as u64).to_le_bytes());

        let mut file = OpenOptions::new()
            .write(true)
            .open(self.root_offset_path.clone())
            .expect("Failed to open root offset file");

        file.write_all(&(root_offset as u64).to_le_bytes());
        // file.sync_all().expect("Failed to sync root offset file");
    }

    pub fn increment_and_allocate_block(&mut self) -> usize {
        let mut used_blocks = self.used_blocks();
        self.set_used_blocks(used_blocks + 1);

        used_blocks
    }

    pub fn store(&mut self, serialized: Vec<u8>, index: usize) -> io::Result<usize> {
        // save to temporary file and atomically rename
        let block_index = if index == 0 {
            self.increment_and_allocate_block()
        } else {
            index
        };

        let temp_file_path = self.path.join(format!("{}.tmp", block_index));
        let file_path = self.path.join(format!("{}.bin", block_index));

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(temp_file_path.clone())
            .expect("Failed to open temp file");

        file.write_all(&serialized)
            .expect("Failed to write to file");

        fs::rename(temp_file_path, file_path).expect("Failed to rename temp file");

        Ok(block_index)
    }

    pub fn load(&self, offset: usize) -> io::Result<Vec<u8>> {
        let file_path = self.path.join(format!("{}.bin", offset));

        if !file_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("File not found: {:?}", file_path),
            ));
        }

        let mut file = OpenOptions::new()
            .read(true)
            .open(file_path.clone())
            .expect(format!("Failed to open file: {:?}", file_path).as_str());

        let mut serialized = Vec::new();

        file.read_to_end(&mut serialized)
            .expect("Failed to read file");

        Ok(serialized)
    }
}
