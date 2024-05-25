use crate::services::LockService;
use memmap::MmapMut;
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::path::PathBuf;

pub struct BlockStorage {
    pub mmap: MmapMut,
    path: PathBuf,
    locks: LockService,
}

/*

    Schema for Block Storage:

    - Header:
        - Used blocks (u64)
        - Root index (u64)
    - Blocks:
        - Block Header:
            - Is primary (u8)
            - Index in chain (u64)
            - Primary index (u64)
            - Next block index (u64)
            - Previous block index (u64)
            - Serialized node length (u64)

        - Data:
            - Node data

*/

pub const SIZE_OF_U64: usize = std::mem::size_of::<u64>();
pub const HEADER_SIZE: usize = SIZE_OF_U64 * 2; // Used space + root offset

pub const BLOCK_SIZE: usize = 4096; // typical page size
pub const BLOCK_HEADER_SIZE: usize = SIZE_OF_U64 * 5 + 1; // Index in chain + Primary index + Next block offset + Previous block offset + Serialized node length + Is primary
pub const BLOCK_DATA_SIZE: usize = BLOCK_SIZE - BLOCK_HEADER_SIZE;

#[derive(Debug, Clone)]
pub struct BlockHeaderData {
    pub is_primary: bool,
    pub index_in_chain: u64,
    pub primary_index: u64,
    pub next_block_offset: u64,
    pub previous_block_offset: u64,
    pub serialized_node_length: u64,
}

impl Copy for BlockHeaderData {}

impl BlockStorage {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let exists = path.exists();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(!exists)
            .open(path.clone())?;

        if !exists {
            file.set_len(1_000_000)?;
        }

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        // take path, remove everything after the last dot (the extension), and add _locks
        let mut locks_path = path.clone().to_str().unwrap().to_string();
        let last_dot = locks_path.rfind('.').unwrap();
        locks_path.replace_range(last_dot.., "_locks");

        fs::create_dir_all(&locks_path).expect("Failed to create directory");

        Ok(BlockStorage {
            mmap,
            path,
            locks: LockService::new(locks_path.into()),
        })
    }

    pub fn used_blocks(&self) -> usize {
        (u64::from_le_bytes(self.mmap[0..SIZE_OF_U64].try_into().unwrap()) as usize) + 1
    }

    pub fn set_used_blocks(&mut self, used_blocks: usize) {
        self.mmap[0..SIZE_OF_U64].copy_from_slice(&(used_blocks as u64).to_le_bytes());
    }

    pub fn root_offset(&self) -> usize {
        u64::from_le_bytes(
            self.mmap[SIZE_OF_U64..(2 * SIZE_OF_U64)]
                .try_into()
                .unwrap(),
        ) as usize
    }

    pub fn set_root_offset(&mut self, root_offset: usize) {
        self.mmap[SIZE_OF_U64..(2 * SIZE_OF_U64)]
            .copy_from_slice(&(root_offset as u64).to_le_bytes());
    }

    pub fn increment_and_allocate_block(&mut self) -> usize {
        let mut used_blocks = self.used_blocks();
        self.set_used_blocks(used_blocks + 1);

        if (used_blocks + 1) * BLOCK_SIZE > self.mmap.len() {
            self.resize_mmap().unwrap();
        }

        used_blocks
    }

    fn resize_mmap(&mut self) -> io::Result<()> {
        println!("Resizing mmap");
        let current_len = self.mmap.len();
        let new_len = current_len * 2;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.path.clone())?;

        file.set_len(new_len as u64)?;

        self.mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(())
    }

    pub fn get_block_header_data(&self, index: usize) -> BlockHeaderData {
        let start = HEADER_SIZE + index * BLOCK_SIZE;
        let end = start + BLOCK_HEADER_SIZE;

        let is_primary = self.mmap[start] == 1;
        let index_in_chain =
            u64::from_le_bytes(self.mmap[start + 1..start + 9].try_into().unwrap());
        let primary_index =
            u64::from_le_bytes(self.mmap[start + 9..start + 17].try_into().unwrap());
        let next_block_offset =
            u64::from_le_bytes(self.mmap[start + 17..start + 25].try_into().unwrap());
        let previous_block_offset =
            u64::from_le_bytes(self.mmap[start + 25..start + 33].try_into().unwrap());
        let serialized_node_length =
            u64::from_le_bytes(self.mmap[start + 33..end].try_into().unwrap());

        BlockHeaderData {
            is_primary,
            index_in_chain,
            primary_index,
            next_block_offset,
            previous_block_offset,
            serialized_node_length,
        }
    }

    pub fn set_block_header_data(&mut self, index: usize, data: BlockHeaderData) {
        let start = HEADER_SIZE + index * BLOCK_SIZE;

        self.mmap[start] = data.is_primary as u8;
        self.mmap[start + 1..start + 9].copy_from_slice(&data.index_in_chain.to_le_bytes());
        self.mmap[start + 9..start + 17].copy_from_slice(&data.primary_index.to_le_bytes());
        self.mmap[start + 17..start + 25].copy_from_slice(&data.next_block_offset.to_le_bytes());
        self.mmap[start + 25..start + 33]
            .copy_from_slice(&data.previous_block_offset.to_le_bytes());
        self.mmap[start + 33..start + 41]
            .copy_from_slice(&data.serialized_node_length.to_le_bytes());
    }

    pub fn get_block_bytes(&self, index: usize) -> &[u8] {
        let start = HEADER_SIZE + index * BLOCK_SIZE + BLOCK_HEADER_SIZE;
        let end = start + BLOCK_DATA_SIZE;

        &self.mmap[start..end]
    }

    pub fn store_block(&mut self, index: usize, data: &[u8]) {
        let start = HEADER_SIZE + index * BLOCK_SIZE + BLOCK_HEADER_SIZE;

        self.mmap[start..start + data.len()].copy_from_slice(data);
    }

    pub fn store(&mut self, serialized: Vec<u8>, index: usize) -> io::Result<usize> {
        let serialized_len = serialized.len() as u64;
        let blocks_required =
            ((serialized_len + BLOCK_DATA_SIZE as u64 - 1) / BLOCK_DATA_SIZE as u64) as usize;

        // Allocate new block if this is a new node
        let mut current_block_index = if index == 0 {
            self.increment_and_allocate_block()
        } else {
            index
        };

        // Initialize writing state
        let mut remaining_bytes_to_write = serialized_len;
        let mut bytes_written = 0;
        let mut prev_block_index = 0;

        let original_block_index = current_block_index;

        self.acquire_lock(original_block_index)?;

        // Clear previous overflow chain if it exists
        let mut used_blocks = Vec::new();
        if index != 0 {
            let mut temp_block_index = index;
            while temp_block_index != 0 {
                let block_header = self.get_block_header_data(temp_block_index);
                used_blocks.push(temp_block_index);
                temp_block_index = block_header.next_block_offset as usize;
                if used_blocks.len() >= blocks_required {
                    break;
                }
            }
        }

        // Clear excess blocks if the node is smaller
        if used_blocks.len() > blocks_required {
            for &index in &used_blocks[blocks_required..] {
                self.clear_block(index);
            }
            used_blocks.truncate(blocks_required);
        }

        // Write new node data into blocks
        for i in 0..blocks_required {
            let bytes_to_write = std::cmp::min(remaining_bytes_to_write as usize, BLOCK_DATA_SIZE);
            self.store_block(
                current_block_index,
                &serialized[bytes_written as usize..bytes_written + bytes_to_write],
            );

            let next_block_index = if remaining_bytes_to_write > bytes_to_write as u64 {
                if i + 1 < used_blocks.len() {
                    used_blocks[i + 1]
                } else {
                    self.increment_and_allocate_block()
                }
            } else {
                0
            };

            let block_header = BlockHeaderData {
                is_primary: i == 0,
                index_in_chain: i as u64,
                primary_index: original_block_index as u64,
                next_block_offset: next_block_index as u64,
                previous_block_offset: if i == 0 { 0 } else { prev_block_index as u64 },
                serialized_node_length: serialized_len,
            };

            self.set_block_header_data(current_block_index, block_header);

            // Debug statements
            // println!(
            //     "Block {}: is_primary={}, index_in_chain={}, primary_index={}, next_block_offset={}, previous_block_offset={}, serialized_node_length={}",
            //     current_block_index,
            //     block_header.is_primary,
            //     block_header.index_in_chain,
            //     block_header.primary_index,
            //     block_header.next_block_offset,
            //     block_header.previous_block_offset,
            //     block_header.serialized_node_length
            // );

            prev_block_index = current_block_index;
            current_block_index = next_block_index;
            remaining_bytes_to_write -= bytes_to_write as u64;
            bytes_written += bytes_to_write;
        }

        if bytes_written != serialized_len as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Bytes written does not match serialized length",
            ));
        }

        if remaining_bytes_to_write != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Remaining bytes to write is not 0",
            ));
        }

        self.release_lock(original_block_index)?;
        Ok(original_block_index)
    }

    fn clear_block(&mut self, index: usize) {
        let start = HEADER_SIZE + index * BLOCK_SIZE;
        let end = start + BLOCK_SIZE;
        self.mmap[start..end].fill(0);
        // Debug statement
        println!("Cleared block at index {}", index);
    }

    pub fn load(&self, offset: usize) -> io::Result<Vec<u8>> {
        let mut current_block_index = offset;
        let mut serialized = Vec::new();

        self.acquire_lock(offset)?;

        loop {
            let block_header = self.get_block_header_data(current_block_index);

            if block_header.is_primary {
                serialized = Vec::with_capacity(block_header.serialized_node_length as usize);
            }

            let data = self.get_block_bytes(current_block_index);
            serialized.extend_from_slice(data);

            // println!(
            //     "LOADING Block {}: is_primary={}, index_in_chain={}, primary_index={}, next_block_offset={}, previous_block_offset={}, serialized_node_length={}",
            //     current_block_index,
            //     block_header.is_primary,
            //     block_header.index_in_chain,
            //     block_header.primary_index,
            //     block_header.next_block_offset,
            //     block_header.previous_block_offset,
            //     block_header.serialized_node_length
            // );

            if block_header.next_block_offset == 0 {
                if serialized.len() < block_header.serialized_node_length as usize {
                    println!("Serialized node length does not match actual length");
                    println!("Serialized length: {}", serialized.len());
                    println!(
                        "Actual length: {}",
                        block_header.serialized_node_length as usize
                    );

                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Serialized node length does not match actual length",
                    ));
                }

                break;
            }

            current_block_index = block_header.next_block_offset as usize;
        }

        self.release_lock(offset)?;

        Ok(serialized)
    }

    pub fn acquire_lock(&self, index: usize) -> io::Result<()> {
        return Ok(());
        self.locks.acquire(index.to_string())
    }

    pub fn release_lock(&self, index: usize) -> io::Result<()> {
        return Ok(());
        self.locks.release(index.to_string())
    }
}
