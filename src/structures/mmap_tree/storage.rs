use crate::services::LockService;

use super::node::Node;
use memmap::MmapMut;
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::path::PathBuf;

use super::serialization::{TreeDeserialization, TreeSerialization};
use std::fmt::Debug;

pub struct StorageManager<K, V> {
    pub mmap: MmapMut,
    pub used_space: usize,
    path: PathBuf,
    phantom: std::marker::PhantomData<(K, V)>,
    locks: LockService,
}

pub const SIZE_OF_USIZE: usize = std::mem::size_of::<usize>();
pub const HEADER_SIZE: usize = SIZE_OF_USIZE * 2; // Used space + root offset

pub const BLOCK_SIZE: usize = 16384;
pub const OVERFLOW_POINTER_SIZE: usize = SIZE_OF_USIZE;
pub const BLOCK_HEADER_SIZE: usize = SIZE_OF_USIZE + 1; // one byte for if it is the primary block or overflow block
pub const BLOCK_DATA_SIZE: usize = BLOCK_SIZE - OVERFLOW_POINTER_SIZE - BLOCK_HEADER_SIZE;

impl<K, V> StorageManager<K, V>
where
    K: Clone + Ord + TreeSerialization + TreeDeserialization + Debug,
    V: Clone + TreeSerialization + TreeDeserialization,
{
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

        let mut manager = StorageManager {
            mmap,
            used_space: 0,
            path,
            phantom: std::marker::PhantomData,
            locks: LockService::new(locks_path.into()),
        };

        let used_space = if exists && manager.mmap.len() > HEADER_SIZE {
            manager.used_space()
        } else {
            0
        };

        // println!("INIT Used space: {}", used_space);

        manager.set_used_space(used_space);

        Ok(manager)
    }

    pub fn store_node(&mut self, node: &mut Node<K, V>) -> io::Result<usize> {
        let serialized = node.serialize();

        // println!("Storing Serialized len: {}", serialized.len());

        let serialized_len = serialized.len();

        let num_blocks_required = (serialized_len + BLOCK_DATA_SIZE - 1) / BLOCK_DATA_SIZE;

        let mut needs_new_blocks = true;

        let mut prev_num_blocks_required = 0;

        if node.offset == 0 {
            node.offset = self.increment_and_allocate_block()?;
            // println!("Allocating block offset: {}", node.offset);
        } else {
            // println!("Using previous node offset: {}", node.offset);
            let prev_serialized_len = usize::from_le_bytes(
                self.read_from_offset(node.offset + 1, SIZE_OF_USIZE)
                    .try_into()
                    .unwrap(),
            );
            prev_num_blocks_required =
                (prev_serialized_len + BLOCK_DATA_SIZE - 1) / BLOCK_DATA_SIZE;
            needs_new_blocks = num_blocks_required > prev_num_blocks_required;

            // println!(
            //     "Prev serialized len: {}, prev num blocks required: {}",
            //     prev_serialized_len, prev_num_blocks_required
            // );
        }

        // println!(
        //     "Storing node at offset: {}, serialized len: {}",
        //     node.offset, serialized_len
        // );

        let mut current_block_offset = node.offset.clone();

        let original_offset = current_block_offset.clone();

        let mut remaining_bytes_to_write = serialized_len;

        let mut serialized_bytes_written = 0;

        let mut is_primary = 1u8;

        let mut blocks_written = 0;

        //

        // println!(
        //     "Num blocks required: {}, num blocks prev: {}, needs new blocks: {}",
        //     num_blocks_required, prev_num_blocks_required, needs_new_blocks
        // );

        self.acquire_block_lock(original_offset)?;

        while remaining_bytes_to_write > 0 {
            let bytes_to_write = std::cmp::min(remaining_bytes_to_write, BLOCK_DATA_SIZE);

            // println!(
            //     "writing is primary: {}, at offset: {}",
            //     is_primary, current_block_offset
            // );

            self.write_to_offset(current_block_offset, is_primary.to_le_bytes().as_ref());

            current_block_offset += 1; // one for the primary byte

            self.write_to_offset(current_block_offset, &serialized_len.to_le_bytes());

            current_block_offset += SIZE_OF_USIZE;
            self.write_to_offset(
                current_block_offset,
                &serialized[serialized_bytes_written..serialized_bytes_written + bytes_to_write],
            );

            blocks_written += 1;
            serialized_bytes_written += bytes_to_write;

            remaining_bytes_to_write -= bytes_to_write;
            // current_block_offset += BLOCK_DATA_SIZE;
            current_block_offset += BLOCK_DATA_SIZE; // Move to the end of written data

            // println!(
            //     "Remaining bytes to write: {}, bytes written: {}",
            //     remaining_bytes_to_write, serialized_bytes_written
            // );

            if remaining_bytes_to_write > 0 {
                let next_block_offset: usize;

                if needs_new_blocks && blocks_written >= prev_num_blocks_required {
                    next_block_offset = self.increment_and_allocate_block()?;

                    self.write_to_offset(current_block_offset, &next_block_offset.to_le_bytes());
                } else {
                    next_block_offset = usize::from_le_bytes(
                        self.read_from_offset(current_block_offset, SIZE_OF_USIZE)
                            .try_into()
                            .unwrap(),
                    );

                    // if next_block_offset == 0 {
                    //     next_block_offset = self.increment_and_allocate_block()?;
                    //     println!("allocating bc 0 Next block offset: {}", next_block_offset);
                    //     self.write_to_offset(
                    //         current_block_offset,
                    //         &next_block_offset.to_le_bytes(),
                    //     );
                    // }

                    // println!("Next block offset: {}", next_block_offset);
                }

                current_block_offset = next_block_offset;
            } else {
                self.write_to_offset(current_block_offset, &0u64.to_le_bytes());

                // println!(
                //     "Setting next block offset to 0 at offset: {}",
                //     current_block_offset
                // );
                // // Clear the remaining unused overflow blocks
                // let mut next_block_offset = usize::from_le_bytes(
                //     self.read_from_offset(current_block_offset, SIZE_OF_USIZE)
                //         .try_into()
                //         .unwrap(),
                // );

                // while next_block_offset != 0 {
                //     let next_next_block_offset = usize::from_le_bytes(
                //         self.read_from_offset(next_block_offset + BLOCK_DATA_SIZE, SIZE_OF_USIZE)
                //             .try_into()
                //             .unwrap(),
                //     );

                //     println!("Clearing next block offset: {}", next_block_offset);

                //     self.write_to_offset(next_block_offset + BLOCK_DATA_SIZE, &0u64.to_le_bytes());

                //     next_block_offset = next_next_block_offset;
                // }
            }

            is_primary = 0;
        }

        self.release_block_lock(original_offset)?;

        Ok(node.offset)
    }

    pub fn load_node(&self, offset: usize) -> io::Result<Node<K, V>> {
        let original_offset = offset.clone();
        let mut offset = offset.clone();

        // println!("Loading node at offset: {}", offset);

        let mut serialized = Vec::new();

        let mut is_primary;

        let mut serialized_len;

        let mut bytes_read = 0;

        self.acquire_block_lock(original_offset)?;

        loop {
            let block_is_primary =
                u8::from_le_bytes(self.read_from_offset(offset, 1).try_into().unwrap());

            if block_is_primary == 0 {
                is_primary = false;
            } else if block_is_primary == 1 {
                is_primary = true;
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid block type",
                ));
            }

            if !is_primary && bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Primary block not found",
                ));
            }

            offset += 1; // one for the primary byte

            serialized_len = usize::from_le_bytes(
                self.read_from_offset(offset, SIZE_OF_USIZE)
                    .try_into()
                    .unwrap(),
            );

            offset += SIZE_OF_USIZE;

            // println!("Serialized len: {}", serialized_len);

            let bytes_to_read = std::cmp::min(serialized_len - bytes_read, BLOCK_DATA_SIZE);
            // println!(
            //     "Bytes read: {}, bytes to read: {}",
            //     bytes_read, bytes_to_read
            // );

            bytes_read += bytes_to_read;

            serialized.extend_from_slice(&self.read_from_offset(offset, bytes_to_read));

            offset += BLOCK_DATA_SIZE;

            let next_block_offset = usize::from_le_bytes(
                self.read_from_offset(offset, SIZE_OF_USIZE)
                    .try_into()
                    .unwrap(),
            );

            // println!("Next block offset: {}", next_block_offset);

            if next_block_offset == 0 {
                break;
            }

            offset = next_block_offset;
        }

        self.release_block_lock(original_offset)?;

        let mut node = Node::deserialize(&serialized);
        node.offset = original_offset;

        Ok(node)
    }

    fn resize_mmap(&mut self) -> io::Result<()> {
        let current_len = self.mmap.len();
        let new_len = current_len * 2;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.path.clone())?; // Ensure this path is handled correctly

        file.set_len(new_len as u64)?;

        self.mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(())
    }

    pub fn used_space(&self) -> usize {
        usize::from_le_bytes(self.read_from_offset(0, SIZE_OF_USIZE).try_into().unwrap())
    }

    pub fn set_used_space(&mut self, used_space: usize) {
        self.write_to_offset(0, &used_space.to_le_bytes());
    }

    pub fn root_offset(&self) -> usize {
        usize::from_le_bytes(
            self.read_from_offset(SIZE_OF_USIZE, SIZE_OF_USIZE)
                .try_into()
                .unwrap(),
        )
        // self.root_offset
    }

    pub fn set_root_offset(&mut self, root_offset: usize) {
        self.write_to_offset(SIZE_OF_USIZE, &root_offset.to_le_bytes());
        // self.root_offset = root_offset;
    }

    pub fn increment_and_allocate_block(&mut self) -> io::Result<usize> {
        let used_space = self.used_space();
        // println!("Used space: {}", used_space);
        self.set_used_space(used_space + BLOCK_SIZE);
        let out = used_space + HEADER_SIZE;
        // println!("Allocating block at offset: {}", out);

        if out + BLOCK_SIZE > self.mmap.len() {
            self.resize_mmap()?;
        }

        Ok(out)
    }

    fn write_to_offset(&mut self, offset: usize, data: &[u8]) {
        self.mmap[offset..offset + data.len()].copy_from_slice(data);
        // self.mmap.flush().unwrap();
    }

    fn read_from_offset(&self, offset: usize, len: usize) -> &[u8] {
        &self.mmap[offset..offset + len]
    }

    fn acquire_block_lock(&self, offset: usize) -> io::Result<()> {
        self.locks.acquire(offset.to_string())?;
        Ok(())
    }

    fn release_block_lock(&self, offset: usize) -> io::Result<()> {
        self.locks.release(offset.to_string())?;
        Ok(())
    }
}
