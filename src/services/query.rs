use rayon::prelude::*;

use super::namespace_state::NamespaceState;
use crate::constants::VECTOR_SIZE;
use crate::math::hamming_distance;
use crate::structures::filters::{Filter, KVPair};
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

        // group contiguous indices to batch get vectors

        let result = self
            .state
            .vectors
            .search(quantized_query_vector, top_k, filters)
            .iter()
            .map(|(_, metadata)| {
                // let mut metadata = metadata.clone();
                // metadata.push(KVPair::new("id".to_string(), id.to_string()));

                metadata.clone()
            })
            .collect();

        Ok(result)
    }
}
