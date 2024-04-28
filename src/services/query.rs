use rayon::prelude::*;

use super::namespace_state::NamespaceState;
use crate::constants::VECTOR_SIZE;
use crate::math::hamming_distance;
use crate::structures::metadata_index::KVPair;
use crate::utils::quantize;
use std::io;
use std::path::PathBuf;

pub struct QueryService {
    pub state: NamespaceState,
}

impl QueryService {
    pub fn new(path: PathBuf, namespace_id: String) -> io::Result<Self> {
        let state = NamespaceState::new(path, namespace_id)?;
        Ok(QueryService { state })
    }

    pub fn query(
        &mut self,
        query_vector: &[f32; VECTOR_SIZE],
        filters: Vec<KVPair>,
        top_k: usize,
    ) -> io::Result<Vec<Vec<KVPair>>> {
        let quantized_query_vector = quantize(query_vector);

        let mut indices = Vec::new();
        let mut ids = Vec::new();

        let mut vectors_duration = std::time::Duration::new(0, 0);
        let mut inverted_index_duration = std::time::Duration::new(0, 0);

        for filter in filters {
            // println!("FILTER: {:?}", filter);
            let r = self.state.inverted_index.get(filter);

            match r {
                Some(item) => {
                    // println!("FOUND ITEM: {:?}", item.indices.len());
                    indices.extend_from_slice(&item.indices);
                    ids.extend_from_slice(&item.ids);
                    // indices = item.indices;
                    // ids = item.ids;
                }
                None => {
                    // return Err(io::Error::new(
                    //     io::ErrorKind::InvalidInput,
                    //     "Filter not found",
                    // ));
                    continue;
                }
            }
        }

        println!("INDICES: {:?}", indices.len());

        // let mut top_k_indices = Vec::with_capacity(top_k);

        // for (i, index) in indices.into_iter().enumerate() {
        //     let vector = self.state.vectors.get(index)?;
        //     let distance = hamming_distance(&quantized_query_vector, vector);

        //     if top_k_indices.len() < top_k {
        //         top_k_indices.push((ids[i].clone(), distance));
        //         top_k_indices.sort_by(|a, b| a.1.cmp(&b.1));
        //     } else {
        //         let worst_best_distance = top_k_indices[top_k_indices.len() - 1].1;
        //         if distance < worst_best_distance {
        //             top_k_indices.pop();
        //             top_k_indices.push((ids[i].clone(), distance));
        //             top_k_indices.sort_by(|a, b| a.1.cmp(&b.1));
        //         }
        //     }
        // }

        // group contiguous indices to batch get vectors

        let mut batch_indices: Vec<Vec<usize>> = Vec::new();

        let mut current_batch = Vec::new();

        for index in indices {
            if current_batch.len() == 0 {
                current_batch.push(index);
            } else {
                let last_index = current_batch[current_batch.len() - 1];
                if index == last_index + 1 {
                    current_batch.push(index);
                } else {
                    batch_indices.push(current_batch);
                    current_batch = Vec::new();
                    current_batch.push(index);
                }
            }
        }

        if current_batch.len() > 0 {
            batch_indices.push(current_batch);
        }

        // println!("BATCH INDICES: {:?}", batch_indices.len());

        let mut top_k_indices = Vec::new();

        for batch in batch_indices {
            let start = std::time::Instant::now();
            let vectors = self.state.vectors.get_contiguous(batch[0], batch.len())?;
            vectors_duration += start.elapsed();

            let start = std::time::Instant::now();

            // for (i, vector) in vectors.iter().enumerate() {
            //     let distance = hamming_distance(&quantized_query_vector, vector);

            //     if top_k_indices.len() < top_k {
            //         top_k_indices.push((ids[batch[i]].clone(), distance));
            //         top_k_indices.sort_by(|a, b| a.1.cmp(&b.1));
            //     } else {
            //         let worst_best_distance = top_k_indices[top_k_indices.len() - 1].1;
            //         if distance < worst_best_distance {
            //             top_k_indices.pop();
            //             top_k_indices.push((ids[batch[i]].clone(), distance));
            //             top_k_indices.sort_by(|a, b| a.1.cmp(&b.1));
            //         }
            //     }
            // }

            top_k_indices.extend(
                vectors
                    .par_iter()
                    .enumerate()
                    .fold(
                        || Vec::new(),
                        |mut acc, (idx, vector)| {
                            let distance = hamming_distance(&quantized_query_vector, vector);

                            if acc.len() < top_k {
                                acc.push((ids[idx], distance));
                                acc.sort();
                            } else {
                                let worst_best_distance = acc[acc.len() - 1].1;
                                if distance < worst_best_distance {
                                    acc.pop();
                                    acc.push((ids[idx], distance));
                                    acc.sort();
                                }
                            }

                            acc
                        },
                    )
                    .reduce(
                        || Vec::new(), // Initializer for the reduce step
                        |mut a, mut b| {
                            // How to combine results from different threads
                            a.append(&mut b);
                            a.sort_by_key(|&(_, dist)| dist); // Sort by distance
                            a.truncate(top_k); // Keep only the top k elements
                            a
                        },
                    ),
            );

            inverted_index_duration += start.elapsed();
        }

        let mut kvs = Vec::new();

        for (id, _) in top_k_indices {
            // println!("ID: {:?}", id);
            let r = self.state.metadata_index.get(id);
            match r {
                Some(item) => {
                    kvs.push(item.kvs);
                }
                None => {
                    // return Err(io::Error::new(
                    //     io::ErrorKind::InvalidInput,
                    //     "Metadata not found",
                    // ));
                    println!("Metadata not found");
                    continue;
                }
            }
        }

        // println!(
        //     "FILTERS: {:?}, VECTORS: {:?}, INVERTED INDEX: {:?}",
        //     filters_duration, vectors_duration, inverted_index_duration
        // );

        Ok(kvs)
    }
}
