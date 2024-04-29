use crate::structures::inverted_index::InvertedIndex;
use crate::structures::metadata_index::KVPair;
use rayon::prelude::*;

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
}

impl Filters {
    pub fn new(indices: Vec<usize>) -> Self {
        Filters {
            current_indices: indices,
        }
    }

    pub fn get_indices(&self) -> Vec<usize> {
        self.current_indices.clone()
    }

    pub fn set_indices(&mut self, indices: Vec<usize>) {
        self.current_indices = indices;
    }

    pub fn intersection(&self, other: &Filters) -> Vec<usize> {
        let intersection: Vec<usize> = self
            .current_indices
            .par_iter()
            .filter(|&i| other.current_indices.contains(i))
            .map(|&i| i)
            .collect();

        intersection
    }

    pub fn union(&self, other: &Filters) -> Vec<usize> {
        let mut union: Vec<usize> = self.current_indices.clone();
        union.extend(other.current_indices.clone());
        union.sort();
        union.dedup();
        union
    }

    pub fn difference(&self, other: &Filters) -> Vec<usize> {
        let difference: Vec<usize> = self
            .current_indices
            .par_iter()
            .filter(|&i| !other.current_indices.contains(i))
            .map(|&i| i)
            .collect();

        difference
    }

    pub fn symmetric_difference(&self, other: &Filters) -> Vec<usize> {
        let difference1: Vec<usize> = self
            .current_indices
            .par_iter()
            .filter(|&i| !other.current_indices.contains(i))
            .map(|&i| i)
            .collect();

        let difference2: Vec<usize> = other
            .current_indices
            .par_iter()
            .filter(|&i| !self.current_indices.contains(i))
            .map(|&i| i)
            .collect();

        let mut symmetric_difference: Vec<usize> = difference1.clone();
        symmetric_difference.extend(difference2.clone());
        symmetric_difference.sort();
        symmetric_difference.dedup();
        symmetric_difference
    }

    pub fn is_subset(&self, other: &Filters) -> bool {
        self.current_indices
            .par_iter()
            .all(|i| other.current_indices.contains(i))
    }

    pub fn is_superset(&self, other: &Filters) -> bool {
        other
            .current_indices
            .par_iter()
            .all(|i| self.current_indices.contains(i))
    }

    pub fn from_index(index: &mut InvertedIndex, key: &KVPair) -> Self {
        match index.get(key.clone()) {
            Some(item) => Filters::new(item.indices),
            None => Filters::new(vec![]),
        }
    }

    // Evaluate a Filter and return the resulting Filters object
    pub fn evaluate(filter: &Filter, index: &mut InvertedIndex) -> Filters {
        match filter {
            Filter::And(filters) => {
                let mut result = Filters::new(vec![]); // Start with an empty set or universal set if applicable
                for f in filters {
                    let current = Filters::evaluate(f, index);
                    println!(
                        "Current AND result for {:?}: {:?}",
                        f,
                        current.get_indices()
                    ); // Debug output

                    if result.current_indices.is_empty() {
                        result = current;
                    } else {
                        result = Filters {
                            current_indices: result.intersection(&current),
                        };
                    }
                }
                println!("Final AND result: {:?}", result.get_indices()); // Debug output

                result
            }
            Filter::Or(filters) => {
                let mut result = Filters::new(vec![]);
                for f in filters {
                    let current = Filters::evaluate(f, index);
                    println!("Current OR result for {:?}: {:?}", f, current.get_indices()); // Debug output

                    result = Filters {
                        current_indices: result.union(&current),
                    };
                }
                println!("Final OR result: {:?}", result.get_indices()); // Debug output

                result
            }
            Filter::In(key, values) => {
                let mut result = Filters::new(vec![]);
                for value in values {
                    let kv_pair = KVPair::new(key.clone(), value.clone()); // Ensure correct KVPair creation
                    let current = Filters::from_index(index, &kv_pair);
                    result = Filters {
                        current_indices: result.union(&current),
                    };
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
