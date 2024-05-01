use rayon::prelude::*;

use super::namespace_state::NamespaceState;
use crate::constants::VECTOR_SIZE;
use crate::math::hamming_distance;
use crate::structures::filters::{Filter, Filters};
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
        filters: &Filter,
        top_k: usize,
    ) -> io::Result<Vec<Vec<KVPair>>> {
        let quantized_query_vector = quantize(query_vector);

        let (indices, ids) =
            Filters::evaluate(filters, &mut self.state.inverted_index).get_indices();

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

        current_batch.sort();
        current_batch.dedup();

        if current_batch.len() > 0 {
            batch_indices.push(current_batch);
        }

        // println!("BATCH INDICES: {:?}", batch_indices.len());

        let mut top_k_indices = Vec::new();

        let top_k_to_use = top_k.min(ids.len());

        for batch in batch_indices {
            let vectors = self.state.vectors.get_contiguous(batch[0], batch.len())?;
            top_k_indices.extend(
                vectors
                    .par_iter()
                    .enumerate()
                    .fold(
                        || Vec::new(),
                        |mut acc, (idx, vector)| {
                            let distance = hamming_distance(&quantized_query_vector, vector);

                            if acc.len() < top_k_to_use {
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
                            a.truncate(top_k_to_use); // Keep only the top k elements
                            a
                        },
                    ),
            );
        }

        let mut kvs = Vec::new();

        for (id, _) in top_k_indices {
            let r = self.state.metadata_index.get(id);
            match r {
                Some(item) => {
                    kvs.push(item.kvs);
                }
                None => {
                    println!("Metadata not found");
                    continue;
                }
            }
        }

        Ok(kvs)
    }
}
