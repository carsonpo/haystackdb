use crate::structures::inverted_index::InvertedIndex;
use crate::structures::metadata_index::KVPair;
use rayon::prelude::*;
use std::collections::HashSet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", content = "args")]
pub enum Filter {
    And(Vec<Filter>),
    Or(Vec<Filter>),
    In(String, Vec<String>), // Assuming first String is the key and Vec<String> is the list of values
    Eq(String, String),      // Assuming first String is the key and second String is the value
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Query {
    filters: Filter,
}

pub struct Filters {
    pub current_indices: Vec<usize>,
    pub current_ids: Vec<u128>,
}

impl Filters {
    pub fn new(indices: Vec<usize>, current_ids: Vec<u128>) -> Self {
        Filters {
            current_indices: indices,
            current_ids: current_ids,
        }
    }

    pub fn get_indices(&self) -> (Vec<usize>, Vec<u128>) {
        (self.current_indices.clone(), self.current_ids.clone())
    }

    pub fn set_indices(&mut self, indices: Vec<usize>, ids: Vec<u128>) {
        self.current_indices = indices;
        self.current_ids = ids;
    }

    pub fn intersection(&self, other: &Filters) -> Filters {
        let intersection_indices: Vec<usize> = self
            .current_indices
            .par_iter()
            .filter(|&x| other.current_indices.contains(x))
            .cloned()
            .collect();

        let intersection_ids: Vec<u128> = self
            .current_ids
            .par_iter()
            .filter(|&x| other.current_ids.contains(x))
            .cloned()
            .collect();

        Filters::new(intersection_indices, intersection_ids)
    }
    pub fn union(&self, other: &Filters) -> Filters {
        let mut union_indices = self.current_indices.clone();
        union_indices.extend(other.current_indices.iter().cloned());
        union_indices.sort_unstable();
        union_indices.dedup();

        let mut union_ids = self.current_ids.clone();
        union_ids.extend(other.current_ids.iter().cloned());
        union_ids.sort_unstable();
        union_ids.dedup();

        Filters::new(union_indices, union_ids)
    }

    pub fn difference(&self, other: &Filters) -> Filters {
        let other_indices_set: HashSet<_> = other.current_indices.iter().collect();
        let difference_indices = self
            .current_indices
            .iter()
            .filter(|&x| !other_indices_set.contains(x))
            .cloned()
            .collect::<Vec<_>>();

        let other_ids_set: HashSet<_> = other.current_ids.iter().collect();
        let difference_ids = self
            .current_ids
            .iter()
            .filter(|&x| !other_ids_set.contains(x))
            .cloned()
            .collect::<Vec<_>>();

        Filters::new(difference_indices, difference_ids)
    }

    pub fn is_subset(&self, other: &Filters) -> bool {
        self.current_indices
            .par_iter()
            .all(|x| other.current_indices.contains(x))
            && self
                .current_ids
                .par_iter()
                .all(|x| other.current_ids.contains(x))
    }

    pub fn is_superset(&self, other: &Filters) -> bool {
        other.is_subset(self)
    }

    pub fn from_index(index: &mut InvertedIndex, key: &KVPair) -> Self {
        match index.get(key.clone()) {
            Some(item) => Filters::new(item.indices, item.ids),
            None => Filters::new(vec![], vec![]),
        }
    }

    // Evaluate a Filter and return the resulting Filters object
    pub fn evaluate(filter: &Filter, index: &mut InvertedIndex) -> Filters {
        match filter {
            Filter::And(filters) => {
                let mut result = Filters::new(vec![], vec![]); // Start with an empty set or universal set if applicable
                for f in filters.iter() {
                    let current = Filters::evaluate(f, index);
                    if result.current_indices.is_empty() && result.current_ids.is_empty() {
                        result = current;
                    } else {
                        result = result.intersection(&current);
                    }
                }
                result
            }
            Filter::Or(filters) => {
                let mut result = Filters::new(vec![], vec![]);
                for f in filters.iter() {
                    let current = Filters::evaluate(f, index);
                    result = result.union(&current);
                }
                result
            }
            Filter::In(key, values) => {
                let mut result = Filters::new(vec![], vec![]);
                for value in values.iter() {
                    let kv_pair = KVPair::new(key.clone(), value.clone()); // Ensure correct KVPair creation
                    let current = Filters::from_index(index, &kv_pair);
                    result = result.union(&current);
                }
                result
            }
            Filter::Eq(key, value) => {
                println!(
                    "Evaluating EQ filter for key: {:?}, value: {:?}",
                    key, value
                ); // Debug output
                let kv_pair = KVPair::new(key.clone(), value.clone()); // Ensure correct KVPair creation
                Filters::from_index(index, &kv_pair)
            }
        }
    }
}
