use rand::seq::SliceRandom;
use rand::thread_rng;
use rand::Rng;

use crate::constants::QUANTIZED_VECTOR_SIZE;

// Function to calculate Hamming distance between two vectors
fn hamming_distance(v1: &[u8; QUANTIZED_VECTOR_SIZE], v2: &[u8; QUANTIZED_VECTOR_SIZE]) -> u32 {
    v1.iter()
        .zip(v2.iter())
        .fold(0, |acc, (&x, &y)| acc + (x ^ y).count_ones())
}

// Function to find the mode of bits at each position for vectors in a cluster
pub fn find_modes(vectors: Vec<[u8; QUANTIZED_VECTOR_SIZE]>) -> [u8; QUANTIZED_VECTOR_SIZE] {
    let mut modes = [0; QUANTIZED_VECTOR_SIZE];
    for i in 0..QUANTIZED_VECTOR_SIZE * 8 {
        let count_ones = vectors
            .iter()
            .filter(|vec| (vec[i / 8] & (1 << (i % 8))) != 0)
            .count();
        if count_ones * 2 > vectors.len() {
            // majority of ones
            modes[i / 8] |= 1 << (i % 8);
        }
    }
    modes
}

// K-modes clustering function
fn k_modes_clustering(
    data: Vec<[u8; QUANTIZED_VECTOR_SIZE]>,
    k: usize,
) -> Vec<[u8; QUANTIZED_VECTOR_SIZE]> {
    assert!(
        data.len() >= k,
        "Not enough data points to form the requested number of clusters."
    );

    let mut centroids: Vec<[u8; QUANTIZED_VECTOR_SIZE]> = Vec::new();
    let mut assignments = vec![0usize; data.len()];

    // Initialize centroids (naively picking the first k elements)
    for i in 0..k {
        centroids.push(data[i].clone());
    }

    let mut change = true;
    while change {
        change = false;

        // Assign data points to centroids
        for (idx, vec) in data.iter().enumerate() {
            let mut min_distance = u32::MAX;
            let mut min_index = 0;
            for (centroid_idx, centroid) in centroids.iter().enumerate() {
                let distance = hamming_distance(&vec, centroid);
                if distance < min_distance {
                    min_distance = distance;
                    min_index = centroid_idx;
                }
            }
            if assignments[idx] != min_index {
                assignments[idx] = min_index;
                change = true;
            }
        }

        // Update centroids
        for centroid_idx in 0..k {
            let cluster: Vec<[u8; QUANTIZED_VECTOR_SIZE]> = data
                .iter()
                .zip(assignments.iter())
                .filter_map(|(vec, &assignment)| {
                    if assignment == centroid_idx {
                        Some(vec.clone())
                    } else {
                        None
                    }
                })
                .map(|vec| vec)
                .collect();

            if !cluster.is_empty() {
                centroids[centroid_idx] = find_modes(cluster);
            }
        }
    }

    centroids
}

pub fn balanced_k_modes(data: Vec<[u8; QUANTIZED_VECTOR_SIZE]>) -> (Vec<usize>, Vec<usize>) {
    let mut centroids: [[u8; QUANTIZED_VECTOR_SIZE]; 2] = [data[0], data[1]]; // Correct syntax for fixed-size array initialization
    let mut cluster_indices = vec![0usize; data.len()];
    let mut changes = true;
    let mut iterations = 0;

    while changes && iterations < 50 {
        // Avoid infinite loops
        changes = false;
        let mut cluster0 = Vec::new();
        let mut cluster1 = Vec::new();

        // Assign vectors to the closest centroid
        for (index, vector) in data.iter().enumerate() {
            let dist0 = hamming_distance(vector, &centroids[0]);
            let dist1 = hamming_distance(vector, &centroids[1]);
            let current_assignment = cluster_indices[index];
            let new_assignment = if dist0 <= dist1 { 0 } else { 1 };

            if new_assignment != current_assignment {
                cluster_indices[index] = new_assignment;
                changes = true;
            }

            if new_assignment == 0 {
                cluster0.push(index);
            } else {
                cluster1.push(index);
            }
        }

        // Update centroids for each cluster
        if !cluster0.is_empty() {
            centroids[0] = find_modes(cluster0.iter().map(|&i| data[i]).collect::<Vec<_>>());
        }
        if !cluster1.is_empty() {
            centroids[1] = find_modes(cluster1.iter().map(|&i| data[i]).collect::<Vec<_>>());
        }

        iterations += 1;
    }

    // Distribute indices evenly
    let (mut indices0, mut indices1) = (Vec::new(), Vec::new());
    for (i, &cluster) in cluster_indices.iter().enumerate() {
        if cluster == 0 && indices0.len() < data.len() / 2 || indices1.len() >= data.len() / 2 {
            indices0.push(i);
        } else {
            indices1.push(i);
        }
    }

    (indices0, indices1)
}

pub fn balanced_k_modes_4(
    mut data: Vec<[u8; QUANTIZED_VECTOR_SIZE]>,
) -> (Vec<usize>, Vec<usize>, Vec<usize>, Vec<usize>) {
    if data.len() < 4 {
        panic!("Not enough data points to initialize four clusters");
    }

    // Improved initial centroid selection using random sampling
    let mut rng = thread_rng();
    let mut centroids = data
        .choose_multiple(&mut rng, 4)
        .cloned()
        .collect::<Vec<_>>();

    let mut cluster_indices = vec![0usize; data.len()];
    let mut changes = true;
    let mut iterations = 0;
    let mut clusters = vec![Vec::new(), Vec::new(), Vec::new(), Vec::new()];

    while changes && iterations < 50 {
        changes = false;
        clusters.iter_mut().for_each(|cluster| cluster.clear());

        // Assign vectors to the closest centroid
        for (index, vector) in data.iter().enumerate() {
            let distances = centroids
                .iter()
                .map(|&centroid| hamming_distance(vector, &centroid))
                .collect::<Vec<_>>();
            let new_assignment = distances
                .iter()
                .enumerate()
                .min_by_key(|&(_, dist)| dist)
                .map(|(idx, _)| idx)
                .unwrap();

            if cluster_indices[index] != new_assignment {
                cluster_indices[index] = new_assignment;
                changes = true;
            }
            clusters[new_assignment].push(index);
        }

        // Update centroids and manage empty clusters immediately
        for (i, cluster) in clusters.iter_mut().enumerate() {
            if cluster.is_empty() {
                // Assign a random vector to an empty cluster
                let random_index = rng.gen_range(0..data.len());
                cluster.push(random_index);
                centroids[i] = data[random_index];
                changes = true;
            } else {
                centroids[i] = find_modes(cluster.iter().map(|&idx| data[idx]).collect::<Vec<_>>());
            }
        }

        iterations += 1;
    }

    // Ensure balanced clusters for final output
    balance_clusters(&mut clusters, data.len(), 4);

    (
        clusters[0].clone(),
        clusters[1].clone(),
        clusters[2].clone(),
        clusters[3].clone(),
    )
}

fn find_modes_bits(vectors: &[[u8; QUANTIZED_VECTOR_SIZE]]) -> [u8; QUANTIZED_VECTOR_SIZE] {
    let mut modes = [0u8; QUANTIZED_VECTOR_SIZE];
    for i in 0..QUANTIZED_VECTOR_SIZE {
        let mut counts = [0usize; 8];
        for vector in vectors {
            for j in 0..8 {
                counts[j] += ((vector[i] >> j) & 1) as usize;
            }
        }
        modes[i] = counts
            .iter()
            .enumerate()
            .map(|(j, &count)| ((count >= vectors.len() / 2) as u8) << j)
            .fold(0, |acc, bit| acc | bit);
    }
    modes
}

fn find_medoid(vectors: &[[u8; QUANTIZED_VECTOR_SIZE]]) -> [u8; QUANTIZED_VECTOR_SIZE] {
    let mut min_sum_distance = u32::MAX;
    let mut medoid = [0u8; QUANTIZED_VECTOR_SIZE];
    for &vector in vectors {
        let sum_distance: u32 = vectors.iter().map(|&v| hamming_distance(&vector, &v)).sum();
        if sum_distance < min_sum_distance {
            min_sum_distance = sum_distance;
            medoid = vector;
        }
    }
    medoid
}

pub fn balanced_k_modes_k_clusters(
    mut data: Vec<[u8; QUANTIZED_VECTOR_SIZE]>,
    k: usize,
) -> Vec<Vec<usize>> {
    if data.len() < k {
        panic!("Not enough data points to initialize the specified number of clusters");
    }

    // Improved initial centroid selection using random sampling
    let mut rng = thread_rng();
    let mut centroids = data
        .choose_multiple(&mut rng, k)
        .cloned()
        .collect::<Vec<_>>();

    let mut cluster_indices = vec![0usize; data.len()];
    let mut changes = true;
    let mut iterations = 0;
    let mut clusters = vec![Vec::new(); k];

    while changes && iterations < 100 {
        // println!("Iteration {}", iterations);
        changes = false;
        clusters.iter_mut().for_each(|cluster| cluster.clear());

        // Assign vectors to the closest centroid
        for (index, vector) in data.iter().enumerate() {
            let distances = centroids
                .iter()
                .map(|&centroid| hamming_distance(vector, &centroid))
                .collect::<Vec<_>>();
            let new_assignment = distances
                .iter()
                .enumerate()
                .min_by_key(|&(_, dist)| dist)
                .map(|(idx, _)| idx)
                .unwrap();

            if cluster_indices[index] != new_assignment {
                cluster_indices[index] = new_assignment;
                changes = true;
            }
            clusters[new_assignment].push(index);
        }

        // Update centroids and manage empty clusters immediately
        for (i, cluster) in clusters.iter_mut().enumerate() {
            if cluster.is_empty() {
                // Assign a random vector to an empty cluster
                let random_index = rng.gen_range(0..data.len());
                cluster.push(random_index);
                centroids[i] = data[random_index];
                changes = true;
            } else {
                let vectors = cluster.iter().map(|&idx| data[idx]).collect::<Vec<_>>();
                centroids[i] = find_medoid(&vectors);
            }
        }

        iterations += 1;
    }

    // Ensure balanced clusters for final output
    // balance_clusters(&mut clusters, data.len(), k);

    clusters
}

fn balance_clusters(clusters: &mut Vec<Vec<usize>>, total: usize, k: usize) {
    let target_size = total / k;
    let mut rng = thread_rng();
    let mut all_indices = clusters
        .iter_mut()
        .flat_map(|cluster| cluster.drain(..))
        .collect::<Vec<usize>>();
    all_indices.shuffle(&mut rng);
    clusters.iter_mut().for_each(|cluster| cluster.clear());

    // Distribute indices to ensure no cluster is empty and all are balanced
    for (i, index) in all_indices.into_iter().enumerate() {
        clusters[i % k].push(index);
    }
}

#[test]
fn test_clustering() {
    let vectors = vec![
        [0u8; QUANTIZED_VECTOR_SIZE],
        [0u8; QUANTIZED_VECTOR_SIZE],
        [0u8; QUANTIZED_VECTOR_SIZE],
        [0u8; QUANTIZED_VECTOR_SIZE],
        [1u8; QUANTIZED_VECTOR_SIZE],
        [1u8; QUANTIZED_VECTOR_SIZE],
        [1u8; QUANTIZED_VECTOR_SIZE],
        [1u8; QUANTIZED_VECTOR_SIZE],
    ];

    let clusters = balanced_k_modes_k_clusters(vectors, 2);

    for cluster in clusters {
        println!("{:?}", cluster);
    }
}
