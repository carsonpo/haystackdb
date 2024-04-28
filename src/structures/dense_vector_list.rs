use crate::constants::QUANTIZED_VECTOR_SIZE;
use memmap::MmapMut;
use std::fs::OpenOptions;
use std::io;
use std::path::PathBuf;

const SIZE_OF_U64: usize = std::mem::size_of::<u64>();
const HEADER_SIZE: usize = SIZE_OF_U64;

pub struct DenseVectorList {
    mmap: MmapMut,
    used_space: usize, // Track the used space within the mmap beyond the header
    pub path: PathBuf,
}

impl DenseVectorList {
    pub fn new(path: PathBuf, elements: u64) -> io::Result<Self> {
        let exists = path.exists();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(!exists)
            .open(path.clone())?;

        if !exists {
            // Set the file size, accounting for the header
            file.set_len(elements * (QUANTIZED_VECTOR_SIZE as u64) + HEADER_SIZE as u64)?;
        }

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        let used_space = if exists && mmap.len() > HEADER_SIZE {
            // Read the existing used space from the file
            let used_bytes = &mmap[0..HEADER_SIZE];
            u64::from_le_bytes(used_bytes.try_into().unwrap()) as usize
        } else {
            0 // No data written yet, or file did not exist
        };

        if !exists {
            // Initialize the header if the file is newly created
            mmap[0..HEADER_SIZE].copy_from_slice(&(used_space as u64).to_le_bytes());
        }

        Ok(DenseVectorList {
            mmap,
            used_space,
            path,
        })
    }

    pub fn push(&mut self, vector: [u8; QUANTIZED_VECTOR_SIZE]) -> io::Result<usize> {
        let offset = self.used_space + HEADER_SIZE;
        let required_space = offset + QUANTIZED_VECTOR_SIZE;

        if required_space > self.mmap.len() {
            self.resize_mmap(required_space * 2)?;
        }

        self.mmap[offset..required_space].copy_from_slice(&vector);
        self.used_space += QUANTIZED_VECTOR_SIZE;
        // Update the header in the mmap
        self.mmap[0..HEADER_SIZE].copy_from_slice(&(self.used_space as u64).to_le_bytes());

        Ok(self.used_space / QUANTIZED_VECTOR_SIZE - 1)
    }

    fn resize_mmap(&mut self, new_len: usize) -> io::Result<()> {
        println!("Resizing mmap in DenseVectorList");

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.path.clone())?; // Ensure this path is handled correctly

        file.set_len(new_len as u64)?;

        self.mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(())
    }

    pub fn batch_push(
        &mut self,
        vectors: Vec<[u8; QUANTIZED_VECTOR_SIZE]>,
    ) -> io::Result<Vec<usize>> {
        let start_offset = self.used_space + HEADER_SIZE;
        let total_size = vectors.len() * QUANTIZED_VECTOR_SIZE;
        let required_space = start_offset + total_size;

        // println!(
        //     "Required space: {}, mmap len: {}",
        //     required_space,
        //     self.mmap.len()
        // );

        if required_space > self.mmap.len() {
            self.resize_mmap(required_space * 2)?;
        }

        // println!("Batch push");

        for (i, vector) in vectors.iter().enumerate() {
            let offset = start_offset + i * QUANTIZED_VECTOR_SIZE;
            self.mmap[offset..offset + QUANTIZED_VECTOR_SIZE].copy_from_slice(vector);
        }

        // println!("Batch push done");

        self.used_space += total_size;
        // Update the header in the mmap
        self.mmap[0..HEADER_SIZE].copy_from_slice(&(self.used_space as u64).to_le_bytes());

        Ok((self.used_space / QUANTIZED_VECTOR_SIZE - vectors.len()
            ..self.used_space / QUANTIZED_VECTOR_SIZE)
            .collect())
    }

    pub fn get(&self, index: usize) -> io::Result<&[u8; QUANTIZED_VECTOR_SIZE]> {
        let offset = HEADER_SIZE + index * QUANTIZED_VECTOR_SIZE;
        let end = offset + QUANTIZED_VECTOR_SIZE;

        if end > self.used_space + HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Index out of bounds",
            ));
        }

        //don't use unsafe
        let bytes = &self.mmap[offset..end];
        let val = bytes.try_into().unwrap();
        Ok(val)
    }

    pub fn get_contiguous(
        &self,
        index: usize,
        num_elements: usize,
    ) -> io::Result<&[[u8; QUANTIZED_VECTOR_SIZE]]> {
        let start = HEADER_SIZE + index * QUANTIZED_VECTOR_SIZE;
        let end = start + num_elements * QUANTIZED_VECTOR_SIZE;

        if end > self.used_space + HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Index out of bounds",
            ));
        }

        // let mut vectors = Vec::with_capacity(num_elements);
        // for i in 0..num_elements {
        //     let offset = HEADER_SIZE + (index + i) * QUANTIZED_VECTOR_SIZE;
        //     vectors.push(self.get(index + i)?);
        // }

        // the indices are contiguous, so we can just get a slice of the mmap
        let vectors: &[[u8; QUANTIZED_VECTOR_SIZE]] = unsafe {
            std::slice::from_raw_parts(
                self.mmap.as_ptr().add(start) as *const [u8; QUANTIZED_VECTOR_SIZE],
                num_elements,
            )
        };

        Ok(vectors)
    }

    pub fn len(&self) -> usize {
        self.used_space / QUANTIZED_VECTOR_SIZE
    }

    pub fn insert(&mut self, index: usize, vector: [u8; QUANTIZED_VECTOR_SIZE]) -> io::Result<()> {
        let offset = HEADER_SIZE + index * QUANTIZED_VECTOR_SIZE;
        let end = offset + QUANTIZED_VECTOR_SIZE;

        if end > self.used_space + HEADER_SIZE {
            self.resize_mmap(end * 2)?;
        }

        self.mmap[offset..end].copy_from_slice(&vector);

        Ok(())
    }
}
