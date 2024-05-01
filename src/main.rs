use env_logger::Builder;
use haystackdb::constants::VECTOR_SIZE;
use haystackdb::services::CommitService;
use haystackdb::services::QueryService;
use haystackdb::structures::filters::Filter as QueryFilter;
use haystackdb::structures::metadata_index::KVPair;
use log::info;
use log::LevelFilter;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::{self, path::PathBuf};
use tokio::time::{interval, Duration};

use std::collections::HashMap;
use tokio::sync::OnceCell;
use warp::Filter;

static ACTIVE_NAMESPACES: OnceCell<Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>> =
    OnceCell::const_new();

#[tokio::main]
async fn main() {
    let mut builder = Builder::new();
    builder
        .format(|buf, record| writeln!(buf, "{}: {}", record.level(), record.args()))
        .filter(None, LevelFilter::Info)
        .init();

    let active_namespaces = ACTIVE_NAMESPACES
        .get_or_init(|| async { Arc::new(Mutex::new(HashMap::new())) })
        .await;

    let search_route = warp::path!("query" / String)
        .and(warp::post())
        .and(warp::body::json())
        .and(with_active_namespaces(active_namespaces.clone()))
        .then(
            |namespace_id: String, body: (Vec<f64>, QueryFilter, usize), active_namespaces| async move {
                let base_path = PathBuf::from(format!("/workspace/data/{}/current", namespace_id.clone()));
                ensure_namespace_initialized(&namespace_id, &active_namespaces, base_path.clone())
                    .await;

                let mut query_service = QueryService::new(base_path, namespace_id.clone()).unwrap();
                let fvec = &body.0;
                let metadata = &body.1;
                let top_k = body.2;

                let mut vec: [f32; VECTOR_SIZE] = [0.0; VECTOR_SIZE];
                fvec.iter()
                    .enumerate()
                    .for_each(|(i, &val)| vec[i] = val as f32);

                let start = std::time::Instant::now();

                let search_result = query_service
                    .query(&vec, metadata, top_k)
                    .expect("Failed to query");

                let duration = start.elapsed();

                println!("Query took {:?} to complete", duration);
                warp::reply::json(&search_result)
            },
        );

    let add_vector_route =
        warp::path!("addVector" / String)
            .and(warp::post())
            .and(warp::body::json())
            .and(with_active_namespaces(active_namespaces.clone()))
            .then(
                |namespace_id: String,
                 body: (Vec<f64>, Vec<KVPair>, String),
                 active_namespaces| async move {
                    let base_path = PathBuf::from(format!(
                        "/workspace/data/{}/current",
                        namespace_id.clone()
                    ));

                    ensure_namespace_initialized(
                        &namespace_id,
                        &active_namespaces,
                        base_path.clone(),
                    )
                    .await;

                    let mut commit_service =
                        CommitService::new(base_path, namespace_id.clone()).unwrap();
                    let fvec = &body.0;
                    let metadata = &body.1;

                    let mut vec: [f32; VECTOR_SIZE] = [0.0; VECTOR_SIZE];
                    fvec.iter()
                        .enumerate()
                        .for_each(|(i, &val)| vec[i] = val as f32);

                    // let id = uuid::Uuid::from_str(id_str).unwrap();
                    commit_service.add_to_wal(vec![vec], vec![metadata.clone()]).expect("Failed to add to WAL");
                    warp::reply::json(&"Success")
                },
            );

    // add a PITR route
    let pitr_route = warp::path!("pitr" / String / String)
        .and(warp::get())
        .and(with_active_namespaces(active_namespaces.clone()))
        .then(
            |namespace_id: String, timestamp: String, active_namespaces| async move {
                println!("PITR for namespace: {}", namespace_id);
                let base_path =
                    PathBuf::from(format!("/workspace/data/{}/current", namespace_id.clone()));

                ensure_namespace_initialized(&namespace_id, &active_namespaces, base_path.clone())
                    .await;

                let mut commit_service =
                    CommitService::new(base_path, namespace_id.clone()).unwrap();

                let timestamp = timestamp.parse::<u64>().unwrap();
                commit_service
                    .recover_point_in_time(timestamp)
                    .expect("Failed to PITR");
                warp::reply::json(&"Success")
            },
        );

    let routes = search_route
        .or(add_vector_route)
        .or(pitr_route)
        .with(warp::cors().allow_any_origin());
    warp::serve(routes).run(([0, 0, 0, 0], 8080)).await;
}

fn with_active_namespaces(
    active_namespaces: Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
) -> impl Filter<
    Extract = (Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,),
    Error = std::convert::Infallible,
> + Clone {
    warp::any().map(move || active_namespaces.clone())
}

async fn ensure_namespace_initialized(
    namespace_id: &String,
    active_namespaces: &Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
    base_path_for_async: PathBuf,
) {
    let mut namespaces = active_namespaces.lock().unwrap();
    if !namespaces.contains_key(namespace_id) {
        let namespace_id_cloned = namespace_id.clone();
        let handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                println!("Committing for namespace {}", namespace_id_cloned);
                let start = std::time::Instant::now();
                let commit_worker = std::sync::Arc::new(std::sync::Mutex::new(
                    CommitService::new(base_path_for_async.clone(), namespace_id_cloned.clone())
                        .unwrap(),
                ));

                commit_worker
                    .lock()
                    .unwrap()
                    .commit()
                    .expect("Failed to commit");
                let duration = start.elapsed();
                info!("Commit worker took {:?} to complete", duration);
            }
        });
        namespaces.insert(namespace_id.clone(), handle);
    }
}
