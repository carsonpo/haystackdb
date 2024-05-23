pub mod k_modes;
pub mod metadata;
pub mod node;
pub mod serialization;
pub mod storage;

use node::{Node, NodeType};
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator};
use storage::StorageManager;

use crate::constants::QUANTIZED_VECTOR_SIZE;
use std::io;

use self::k_modes::find_modes;
use self::metadata::{NodeMetadata, NodeMetadataIndex};
use self::node::Vector;
use crate::math::hamming_distance;

use super::filters::{combine_filters, Filter, Filters};
// use super::metadata_index::{KVPair, KVValue};
use super::mmap_tree::serialization::{TreeDeserialization, TreeSerialization};
use crate::structures::filters::{calc_metadata_index_for_metadata, KVPair, KVValue};

use rayon::prelude::*;

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use std::fmt::{Debug, Display};
use std::path::PathBuf;

pub struct ANNTree {
    pub k: usize,
    pub storage_manager: storage::StorageManager,
}

#[derive(Eq, PartialEq)]
struct PathNode {
    distance: u16,
    offset: usize,
}

// Implement `Ord` and `PartialOrd` for `PathNode` to use it in a min-heap
impl Ord for PathNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.distance.cmp(&self.distance) // Reverse order for min-heap
    }
}

impl PartialOrd for PathNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl ANNTree {
    pub fn new(path: PathBuf) -> Result<Self, io::Error> {
        let mut storage_manager =
            StorageManager::new(path).expect("Failed to make storage manager in ANN Tree");

        // println!("INIT Used space: {}", storage_manager.used_space);

        if storage_manager.root_offset() != 0 {
            return Ok(ANNTree {
                storage_manager,
                k: crate::constants::K,
            });
        }

        let mut root = Node::new_leaf();
        root.is_root = true;

        storage_manager.store_node(&mut root)?;
        storage_manager.set_root_offset(root.offset);

        Ok(ANNTree {
            storage_manager,
            k: crate::constants::K,
        })
    }

    pub fn batch_insert(
        &mut self,
        vectors: Vec<Vector>,
        ids: Vec<u128>,
        metadata: Vec<Vec<KVPair>>,
    ) {
        for ((vector, id), metadata) in vectors.iter().zip(ids.iter()).zip(metadata.iter()) {
            self.insert(vector.clone(), *id, metadata.clone());
        }
    }

    pub fn bulk_insert(
        &mut self,
        vectors: Vec<Vector>,
        ids: Vec<u128>,
        metadata: Vec<Vec<KVPair>>,
    ) {
        let mut current_leaves = Vec::new();
        self.collect_leaf_nodes(self.storage_manager.root_offset(), &mut current_leaves)
            .expect("Failed to collect leaf nodes");

        let mut new_root = Node::new_internal();

        new_root.is_root = true;

        self.storage_manager.store_node(&mut new_root).unwrap();

        self.storage_manager.set_root_offset(new_root.offset);

        // self.storage_manager.set_root_offset(leaf.offset);

        println!("Current leaves: {:?}", current_leaves.len());

        // for leaf in current_leaves.iter_mut() {
        //     if leaf.is_root {
        //         leaf.is_root = false;
        //     }
        //     leaf.parent_offset = Some(leaf.offset);
        //     leaf.children.push(leaf.offset);
        //     leaf.vectors.push(find_modes(leaf.vectors.clone()));
        //     self.storage_manager.store_node(leaf).unwrap();
        // }

        let mut all_vectors = Vec::new();
        let mut all_ids = Vec::new();
        let mut all_metadata = Vec::new();

        for leaf in current_leaves.iter_mut() {
            leaf.is_root = false;
            self.storage_manager.store_node(leaf).unwrap();
            all_vectors.extend(leaf.vectors.clone());
            all_ids.extend(leaf.ids.clone());
            all_metadata.extend(leaf.metadata.clone());
        }

        all_vectors.extend(vectors);
        all_ids.extend(ids);
        all_metadata.extend(metadata);

        println!("All vectors: {:?}", all_vectors.len());
        println!("All ids: {:?}", all_ids.len());
        println!("All metadata: {:?}", all_metadata.len());

        let mut leaf = Node::new_leaf();

        for ((vector, id), metadata) in all_vectors
            .iter()
            .zip(all_ids.iter())
            .zip(all_metadata.iter())
        {
            if leaf.is_full() {
                leaf.parent_offset = Some(new_root.offset);
                leaf.node_metadata = calc_metadata_index_for_metadata(leaf.metadata.clone());
                self.storage_manager.store_node(&mut leaf).unwrap();
                new_root.children.push(leaf.offset);
                new_root.vectors.push(find_modes(leaf.vectors.clone()));
                self.storage_manager.store_node(&mut new_root).unwrap();
                leaf = Node::new_leaf();
            }
            leaf.vectors.push(vector.clone());
            leaf.ids.push(*id);
            leaf.metadata.push(metadata.clone());
        }

        new_root.node_metadata = self.compute_node_metadata(&new_root);

        self.storage_manager.store_node(&mut new_root).unwrap();

        self.storage_manager.set_root_offset(new_root.offset);

        // self.true_calibrate();

        // self.summarize_tree();
    }

    pub fn insert(&mut self, vector: Vector, id: u128, metadata: Vec<KVPair>) {
        let entrypoint = self.find_entrypoint(vector);
        let mut node = self.storage_manager.load_node(entrypoint).unwrap();

        // println!("Entrypoint: {:?}", entrypoint);

        if node.is_full() {
            let mut siblings = node.split().expect("Failed to split node");
            let sibling_offsets: Vec<usize> = siblings
                .iter_mut()
                .map(|sibling| {
                    sibling.parent_offset = node.parent_offset; // Set parent offset before storing
                    sibling.node_metadata = self.compute_node_metadata(&sibling);
                    self.storage_manager.store_node(sibling).unwrap()
                })
                .collect();

            for sibling in siblings.clone() {
                if sibling.node_type == NodeType::Internal
                    && sibling.children.len() != sibling.vectors.len()
                {
                    panic!("Internal node has different number of children and vectors");
                }
            }

            if node.is_root {
                let mut new_root = Node::new_internal();
                new_root.is_root = true;
                new_root.children.push(node.offset);
                new_root.vectors.push(find_modes(node.vectors.clone()));
                for sibling_offset in &sibling_offsets {
                    let sibling = self.storage_manager.load_node(*sibling_offset).unwrap();
                    new_root.vectors.push(find_modes(sibling.vectors));
                    new_root.children.push(*sibling_offset);
                }
                self.storage_manager.store_node(&mut new_root).unwrap();
                self.storage_manager.set_root_offset(new_root.offset);
                node.is_root = false;
                node.parent_offset = Some(new_root.offset);
                siblings
                    .iter_mut()
                    .for_each(|sibling| sibling.parent_offset = Some(new_root.offset));
                self.storage_manager.store_node(&mut node).unwrap();
                siblings.iter_mut().for_each(|sibling| {
                    if sibling.node_type == NodeType::Internal
                        && sibling.children.len() != sibling.vectors.len()
                    {
                        panic!("Internal node has different number of children and vectors v3");
                    }
                    sibling.node_metadata = self.compute_node_metadata(sibling);
                    self.storage_manager.store_node(sibling).unwrap();
                });
            } else {
                let parent_offset = node.parent_offset.unwrap();
                let mut parent = self.storage_manager.load_node(parent_offset).unwrap();
                parent.children.push(node.offset);
                parent.vectors.push(find_modes(node.vectors.clone()));
                sibling_offsets
                    .iter()
                    .for_each(|&offset| parent.children.push(offset));
                siblings
                    .iter()
                    .for_each(|sibling| parent.vectors.push(find_modes(sibling.vectors.clone())));
                if parent.node_type == NodeType::Internal
                    && parent.children.len() != parent.vectors.len()
                {
                    println!("Parent vectors: {:?}", parent.vectors.len());
                    println!("Parent children: {:?}", parent.children);
                    println!("Sibling offsets: {:?}", sibling_offsets.len());

                    panic!("parent node has different number of children and vectors");
                }
                self.storage_manager.store_node(&mut parent).unwrap();
                node.parent_offset = Some(parent_offset);
                self.storage_manager.store_node(&mut node).unwrap();
                siblings.into_iter().for_each(|mut sibling| {
                    if sibling.node_type == NodeType::Internal
                        && sibling.children.len() != sibling.vectors.len()
                    {
                        panic!("Internal node has different number of children and vectors v3");
                    }
                    sibling.parent_offset = Some(parent_offset);
                    sibling.node_metadata = self.compute_node_metadata(&sibling);
                    self.storage_manager.store_node(&mut sibling).unwrap();
                });

                let mut current_node = parent;
                while current_node.is_full() {
                    println!("Current node is full");
                    let mut siblings = current_node.split().expect("Failed to split node");
                    let sibling_offsets: Vec<usize> = siblings
                        .iter_mut()
                        .map(|sibling| {
                            sibling.parent_offset = current_node.parent_offset;
                            sibling.node_metadata = self.compute_node_metadata(sibling);
                            self.storage_manager.store_node(sibling).unwrap()
                        })
                        .collect();

                    for sibling in siblings.clone() {
                        if sibling.node_type == NodeType::Internal
                            && sibling.children.len() != sibling.vectors.len()
                        {
                            panic!("Internal node has different number of children and vectors v2");
                        }
                    }

                    if current_node.is_root {
                        let mut new_root = Node::new_internal();
                        new_root.is_root = true;
                        new_root.children.push(current_node.offset);
                        new_root.children.extend(sibling_offsets.clone());
                        new_root
                            .vectors
                            .push(find_modes(current_node.vectors.clone()));
                        siblings.iter().for_each(|sibling| {
                            new_root.vectors.push(find_modes(sibling.vectors.clone()))
                        });
                        self.storage_manager.store_node(&mut new_root).unwrap();
                        self.storage_manager.set_root_offset(new_root.offset);
                        current_node.is_root = false;
                        current_node.parent_offset = Some(new_root.offset);
                        siblings
                            .iter_mut()
                            .for_each(|sibling| sibling.parent_offset = Some(new_root.offset));
                        self.storage_manager.store_node(&mut current_node).unwrap();
                        siblings.into_iter().for_each(|mut sibling| {
                            if sibling.node_type == NodeType::Internal
                                && sibling.children.len() != sibling.vectors.len()
                            {
                                panic!(
                                    "Internal node has different number of children and vectors v4"
                                );
                            }
                            sibling.node_metadata = self.compute_node_metadata(&sibling);
                            self.storage_manager.store_node(&mut sibling).unwrap();
                        });
                        new_root.node_metadata = self.compute_node_metadata(&new_root);
                    } else {
                        let parent_offset = current_node.parent_offset.unwrap();
                        let mut parent = self.storage_manager.load_node(parent_offset).unwrap();
                        parent.children.push(current_node.offset);
                        sibling_offsets
                            .iter()
                            .for_each(|&offset| parent.children.push(offset));
                        parent
                            .vectors
                            .push(find_modes(current_node.vectors.clone()));
                        siblings.iter().for_each(|sibling| {
                            if sibling.node_type == NodeType::Internal
                                && sibling.children.len() != sibling.vectors.len()
                            {
                                panic!(
                                    "Internal node has different number of children and vectors v5"
                                );
                            }
                            parent.vectors.push(find_modes(sibling.vectors.clone()))
                        });
                        self.storage_manager.store_node(&mut parent).unwrap();
                        current_node.parent_offset = Some(parent_offset);
                        self.storage_manager.store_node(&mut current_node).unwrap();
                        siblings.into_iter().for_each(|mut sibling| {
                            sibling.parent_offset = Some(parent_offset);
                            sibling.node_metadata = self.compute_node_metadata(&sibling);
                            self.storage_manager.store_node(&mut sibling).unwrap();
                        });
                        parent.node_metadata = self.compute_node_metadata(&parent);
                        current_node = parent;
                    }
                }
            }
        } else {
            if node.node_type != NodeType::Leaf {
                panic!("Entrypoint is not a leaf node");
            }
            node.vectors.push(vector);
            node.ids.push(id);
            node.metadata.push(metadata.clone());
            for kv in metadata {
                match node.node_metadata.get(kv.key.clone()) {
                    Some(res) => {
                        let mut set = res.clone();
                        match kv.value {
                            KVValue::String(value) => {
                                set.values.insert(value);
                            }
                            KVValue::Float(val) => {
                                let mut float_range = set.float_range.unwrap_or((val, val));
                                if val < float_range.0 {
                                    float_range.0 = val;
                                }
                                if val > float_range.1 {
                                    float_range.1 = val;
                                }
                                set.float_range = Some(float_range);
                            }
                            KVValue::Integer(val) => {
                                let mut int_range = set.int_range.unwrap_or((val, val));
                                if val < int_range.0 {
                                    int_range.0 = val;
                                }
                                if val > int_range.1 {
                                    int_range.1 = val;
                                }
                                set.int_range = Some(int_range);
                            }
                        }

                        node.node_metadata.insert(kv.key.clone(), set);
                    }
                    None => {
                        let mut set = NodeMetadata::new();
                        match kv.value {
                            KVValue::String(v) => {
                                set.values.insert(v);
                            }
                            KVValue::Float(val) => {
                                set.float_range = Some((val, val));
                            }
                            KVValue::Integer(val) => {
                                set.int_range = Some((val, val));
                            }
                        }

                        node.node_metadata.insert(kv.key.clone(), set);
                    }
                }
            }
            self.storage_manager.store_node(&mut node).unwrap();
        }
    }

    fn find_entrypoint(&mut self, vector: Vector) -> usize {
        let mut node = self
            .storage_manager
            .load_node(self.storage_manager.root_offset())
            .unwrap();

        while node.node_type == NodeType::Internal {
            let mut distances: Vec<(usize, u16)> = node
                .vectors
                .par_iter()
                .map(|key| hamming_distance(&vector, key))
                .enumerate()
                .collect();

            distances.sort_by_key(|&(_, distance)| distance);

            let best = distances.get(0).unwrap();

            let best_node = self
                .storage_manager
                .load_node(node.children[best.0])
                .unwrap();

            node = best_node;
        }

        // Now node is a leaf node
        node.offset
    }

    pub fn search(
        &mut self,
        vector: Vector,
        top_k: usize,
        filters: &Filter,
    ) -> Vec<(u128, Vec<KVPair>)> {
        let node = self
            .storage_manager
            .load_node(self.storage_manager.root_offset())
            .unwrap();

        // let mut visited = HashSet::new();

        // println!(
        //     "Root node: {:?}, {:?}",
        //     node.offset,
        //     self.storage_manager.root_offset()
        // );

        let mut candidates = self.traverse(&vector, &node, top_k, filters, 0);

        // Sort by distance and truncate to top_k results
        candidates.sort_by_key(|&(_, distance, _)| distance);
        candidates.truncate(top_k);

        candidates
            .into_iter()
            .map(|(id, _, pairs)| (id, pairs))
            .collect()
    }

    fn traverse(
        &self,
        vector: &Vector,
        node: &Node,
        k: usize,
        filters: &Filter,
        depth: usize,
    ) -> Vec<(u128, u16, Vec<KVPair>)> {
        if node.node_type == NodeType::Leaf {
            // println!("Leaf node: {:?}", node.offset);
            return self
                .collect_top_k_with_filters(vector, &node.vectors, &node.metadata, filters, k)
                .par_iter()
                .map(|(idx, distance)| {
                    let id = node.ids[*idx];
                    let metadata = node.metadata[*idx].clone();
                    (id, *distance, metadata)
                })
                .collect::<Vec<_>>();
            // .into_iter()
            // .filter(|(id, _, _)| visited.insert(*id))
            // .collect();
        }

        let mut alpha = crate::constants::ALPHA;
        if alpha <= 1 {
            alpha = 1;
        }

        // Collect top alpha nodes
        let best_children: Vec<(usize, u16, Node)> =
            self.collect_top_k_with_nodes(vector, &node.vectors, &node.children, filters, alpha);

        // println!("Best children: {:?}", best_children.len());

        // Track results from top alpha nodes
        // let mut all_results: Vec<(u128, u16, Vec<KVPair>)> = vec![];
        // for (_, _, child_node) in best_children.iter() {
        //     let mut current_results =
        //         self.traverse(vector, child_node, k, filters, depth + 1, visited);
        //     all_results.append(&mut current_results);
        // }
        let mut all_results: Vec<_> = best_children
            .par_iter()
            .flat_map(|(_, _, child_node)| self.traverse(vector, child_node, k, filters, depth + 1))
            .collect();

        // Evaluate paths and choose the best based on distance
        all_results.sort_by_key(|&(_, distance, _)| distance);
        all_results.truncate(alpha);

        all_results
    }

    fn collect_top_k_with_filters(
        &self,
        query: &Vector,
        vector_items: &Vec<Vector>,
        metadata_items: &Vec<Vec<KVPair>>,
        filters: &Filter,
        k: usize,
    ) -> Vec<(usize, u16)> {
        let mut top_k_values: Vec<(usize, u16)> = Vec::with_capacity(k);

        let mut distances: Vec<(usize, u16)> = vector_items
            .par_iter()
            .enumerate()
            .map(|(idx, item)| (idx, hamming_distance(item, query)))
            .collect();

        // Sort distances to find the top-k closest items
        distances.sort_by_key(|&(_, distance)| distance);

        // Load nodes and filter top-k items
        for &(idx, distance) in distances.iter() {
            if top_k_values.len() >= k && distance >= top_k_values[k - 1].1 {
                break; // No need to check further if we already have top-k and current distance is not better
            }

            // Evaluate filters for the loaded node
            if !Filters::should_prune_metadata(filters, &&metadata_items[idx]) {
                // Add to top-k if it matches the filter
                if top_k_values.len() < k {
                    top_k_values.push((idx, distance));
                } else {
                    // Replace the worst in top-k if current distance is better
                    let worst_best_distance = top_k_values[k - 1].1;
                    if distance < worst_best_distance {
                        top_k_values.pop();
                        top_k_values.push((idx, distance));
                        top_k_values.sort_by_key(|&(_, distance)| distance);
                    }
                }
            } else {
                // println!("Pruned");
            }
        }

        top_k_values
    }

    fn collect_top_k_with_nodes(
        &self,
        query: &Vector,
        items: &Vec<Vector>,
        children: &Vec<usize>,
        filters: &Filter,
        k: usize,
    ) -> Vec<(usize, u16, Node)> {
        let mut top_k_values: Vec<(usize, u16, Node)> = Vec::with_capacity(k);

        let mut distances: Vec<(usize, u16)> = items
            .par_iter()
            .enumerate()
            .map(|(idx, item)| (idx, hamming_distance(item, query)))
            .collect();

        // Sort distances to find the top-k closest items
        distances.sort_by_key(|&(_, distance)| distance);

        // Load nodes and filter top-k items
        for &(idx, distance) in distances.iter() {
            if top_k_values.len() >= k && distance >= top_k_values[k - 1].1 {
                break; // No need to check further if we already have top-k and current distance is not better
            }

            let child_node = self.storage_manager.load_node(children[idx]).unwrap();

            // Evaluate filters for the loaded node
            if !Filters::should_prune(filters, &child_node.node_metadata) {
                // Add to top-k if it matches the filter
                if top_k_values.len() < k {
                    top_k_values.push((idx, distance, child_node));
                } else {
                    // Replace the worst in top-k if current distance is better
                    let worst_best_distance = top_k_values[k - 1].1;
                    if distance < worst_best_distance {
                        top_k_values.pop();
                        top_k_values.push((idx, distance, child_node));
                        top_k_values.sort_by_key(|&(_, distance, _)| distance);
                    }
                }
            } else {
                // println!("Pruned");
                // println!("Filters: {:?}", filters);
                // println!("Node metadata: {:?}", child_node.node_metadata);
            }
        }

        top_k_values
    }

    fn collect_leaf_nodes(
        &mut self,
        offset: usize,
        leaf_nodes: &mut Vec<Node>,
    ) -> Result<(), io::Error> {
        let node = self.storage_manager.load_node(offset).unwrap().clone();
        if node.node_type == NodeType::Leaf {
            leaf_nodes.push(node);
        } else {
            for &child_offset in &node.children {
                self.collect_leaf_nodes(child_offset, leaf_nodes)?;
            }
        }
        Ok(())
    }

    pub fn true_calibrate(&mut self) -> Result<(), io::Error> {
        // Step 1: Get all leaf nodes
        let mut leaf_nodes = Vec::new();
        self.collect_leaf_nodes(self.storage_manager.root_offset(), &mut leaf_nodes)?;

        // Step 2: Make a new root
        let mut new_root = Node::new_internal();
        new_root.is_root = true;

        // Step 3: Store the new root to set its offset
        self.storage_manager.store_node(&mut new_root)?;
        self.storage_manager.set_root_offset(new_root.offset);

        // Step 4: Make all the leaf nodes the new root's children, and set all their parent_offsets to the new root's offset
        for leaf_node in &mut leaf_nodes {
            leaf_node.parent_offset = Some(new_root.offset);
            new_root.children.push(leaf_node.offset);
            new_root.vectors.push(find_modes(leaf_node.vectors.clone()));
            // new_root.node_metadata = self.compute_node_metadata(&new_root);
            self.storage_manager.store_node(leaf_node)?;
        }

        new_root.node_metadata = self.compute_node_metadata(&new_root);

        // new_root.node_metadata = combine_filters(
        //     leaf_nodes
        //         .iter()
        //         .map(|node| node.node_metadata.clone())
        //         .collect(),
        // );

        // Update the root node with its children and vectors
        self.storage_manager.store_node(&mut new_root)?;

        // Step 5: Split the nodes until it is balanced/there are no nodes that are full
        let mut current_nodes = vec![new_root];
        while let Some(mut node) = current_nodes.pop() {
            if node.is_full() {
                let mut siblings = node.split().expect("Failed to split node");
                let sibling_offsets: Vec<usize> = siblings
                    .iter_mut()
                    .map(|sibling| {
                        sibling.parent_offset = node.parent_offset; // Set parent offset before storing
                        sibling.node_metadata = self.compute_node_metadata(sibling);
                        self.storage_manager.store_node(sibling).unwrap()
                    })
                    .collect();

                for sibling in siblings.clone() {
                    if sibling.node_type == NodeType::Internal
                        && sibling.children.len() != sibling.vectors.len()
                    {
                        panic!("Internal node has different number of children and vectors");
                    }
                }

                if node.is_root {
                    let mut new_root = Node::new_internal();
                    new_root.is_root = true;
                    new_root.children.push(node.offset);
                    new_root.vectors.push(find_modes(node.vectors.clone()));

                    for sibling_offset in &sibling_offsets {
                        let sibling = self
                            .storage_manager
                            .load_node(*sibling_offset)
                            .unwrap()
                            .clone();
                        new_root.vectors.push(find_modes(sibling.vectors));
                        new_root.children.push(*sibling_offset);
                    }

                    new_root.node_metadata = self.compute_node_metadata(&new_root);
                    self.storage_manager.store_node(&mut new_root)?;
                    self.storage_manager.set_root_offset(new_root.offset);
                    node.is_root = false;
                    node.parent_offset = Some(new_root.offset);
                    self.storage_manager.store_node(&mut node)?;
                    siblings
                        .iter_mut()
                        .for_each(|sibling| sibling.parent_offset = Some(new_root.offset));
                    self.storage_manager.store_node(&mut node)?;
                    siblings.iter_mut().for_each(|sibling| {
                        if sibling.node_type == NodeType::Internal
                            && sibling.children.len() != sibling.vectors.len()
                        {
                            panic!("Internal node has different number of children and vectors v3");
                        }
                        sibling.node_metadata = self.compute_node_metadata(sibling);
                        self.storage_manager.store_node(sibling);
                    });
                } else {
                    let parent_offset = node.parent_offset.unwrap();
                    let mut parent = self
                        .storage_manager
                        .load_node(parent_offset)
                        .unwrap()
                        .clone();
                    parent.children.push(node.offset);
                    parent.vectors.push(find_modes(node.vectors.clone()));
                    sibling_offsets
                        .iter()
                        .for_each(|&offset| parent.children.push(offset));
                    siblings.iter().for_each(|sibling| {
                        parent.vectors.push(find_modes(sibling.vectors.clone()))
                    });
                    parent.node_metadata = self.compute_node_metadata(&parent);

                    if parent.node_type == NodeType::Internal
                        && parent.children.len() != parent.vectors.len()
                    {
                        panic!("parent node has different number of children and vectors");
                    }
                    self.storage_manager.store_node(&mut parent)?;
                    node.parent_offset = Some(parent_offset);
                    self.storage_manager.store_node(&mut node)?;
                    siblings.into_iter().for_each(|mut sibling| {
                        if sibling.node_type == NodeType::Internal
                            && sibling.children.len() != sibling.vectors.len()
                        {
                            panic!("Internal node has different number of children and vectors v3");
                        }
                        sibling.parent_offset = Some(parent_offset);
                        sibling.node_metadata = self.compute_node_metadata(&sibling);
                        self.storage_manager.store_node(&mut sibling);
                    });

                    let mut current_node = parent;
                    while current_node.is_full() {
                        let mut siblings = current_node.split().expect("Failed to split node");
                        let sibling_offsets: Vec<usize> = siblings
                            .iter_mut()
                            .map(|sibling| {
                                sibling.parent_offset = Some(current_node.parent_offset.unwrap());
                                sibling.node_metadata = self.compute_node_metadata(sibling);
                                self.storage_manager.store_node(sibling).unwrap()
                            })
                            .collect();

                        for sibling in siblings.clone() {
                            if sibling.node_type == NodeType::Internal
                                && sibling.children.len() != sibling.vectors.len()
                            {
                                panic!(
                                    "Internal node has different number of children and vectors v2"
                                );
                            }
                        }

                        if current_node.is_root {
                            let mut new_root = Node::new_internal();
                            new_root.is_root = true;
                            new_root.children.push(current_node.offset);
                            new_root.children.extend(sibling_offsets.clone());
                            new_root
                                .vectors
                                .push(find_modes(current_node.vectors.clone()));
                            siblings.iter().for_each(|sibling| {
                                new_root.vectors.push(find_modes(sibling.vectors.clone()))
                            });
                            new_root.node_metadata = self.compute_node_metadata(&new_root);
                            self.storage_manager.store_node(&mut new_root)?;
                            self.storage_manager.set_root_offset(new_root.offset);
                            current_node.is_root = false;
                            current_node.parent_offset = Some(new_root.offset);
                            siblings
                                .iter_mut()
                                .for_each(|sibling| sibling.parent_offset = Some(new_root.offset));
                            self.storage_manager.store_node(&mut current_node)?;
                            siblings.into_iter().for_each(|mut sibling| {
                                if sibling.node_type == NodeType::Internal && sibling.children.len() != sibling.vectors.len() {
                                    panic!("Internal node has different number of children and vectors v4");
                                }
                                sibling.node_metadata = self.compute_node_metadata(&sibling);
                                self.storage_manager.store_node(&mut sibling);
                            });
                        } else {
                            let parent_offset = current_node.parent_offset.unwrap();
                            let mut parent = self
                                .storage_manager
                                .load_node(parent_offset)
                                .unwrap()
                                .clone();
                            parent.children.push(current_node.offset);
                            sibling_offsets
                                .iter()
                                .for_each(|&offset| parent.children.push(offset));
                            parent
                                .vectors
                                .push(find_modes(current_node.vectors.clone()));
                            siblings.iter_mut().for_each(|sibling| {
                                if sibling.node_type == NodeType::Internal && sibling.children.len() != sibling.vectors.len() {
                                    panic!("Internal node has different number of children and vectors v5");
                                }
                                sibling.node_metadata = self.compute_node_metadata(sibling);
                                parent.vectors.push(find_modes(sibling.vectors.clone()))
                            });
                            parent.node_metadata = self.compute_node_metadata(&parent);
                            self.storage_manager.store_node(&mut parent)?;
                            current_node.parent_offset = Some(parent_offset);
                            current_node.node_metadata = self.compute_node_metadata(&current_node);
                            self.storage_manager.store_node(&mut current_node)?;
                            siblings.into_iter().for_each(|mut sibling| {
                                sibling.parent_offset = Some(parent_offset);
                                sibling.node_metadata = self.compute_node_metadata(&sibling);
                                self.storage_manager.store_node(&mut sibling);
                            });
                            current_node = parent.clone();
                            current_node.node_metadata = self.compute_node_metadata(&current_node);
                        }
                    }
                }
            }

            node.node_metadata = self.compute_node_metadata(&node);
        }
        Ok(())
    }

    fn compute_node_metadata(&self, node: &Node) -> NodeMetadataIndex {
        let mut children_metadatas = Vec::new();

        for child_offset in &node.children {
            let child = self.storage_manager.load_node(*child_offset).unwrap();

            children_metadatas.push(child.node_metadata);
        }

        combine_filters(children_metadatas)
    }

    pub fn summarize_tree(&self) {
        let mut queue = vec![self.storage_manager.root_offset()];
        let mut depth = 0;

        while !queue.is_empty() {
            let mut next_queue = Vec::new();

            for offset in queue {
                let node = self.storage_manager.load_node(offset).unwrap();
                println!(
                    "Depth: {}, Node type: {:?}, Offset: {}, Children: {}, Vectors: {}",
                    depth,
                    node.node_type,
                    node.offset,
                    node.children.len(),
                    node.vectors.len()
                );

                if node.node_type == NodeType::Internal {
                    next_queue.extend(node.children.clone());
                }
            }

            queue = next_queue;
            depth += 1;
        }

        println!("Tree depth: {}", depth);
    }
}
