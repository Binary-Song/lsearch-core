use crate::core::{
    index::{index_directory, IndexArgs},
    search::{search_in_index_files, SearchArgs, SearchResult},
};
use console_subscriber;
use futures::{Sink, SinkExt};
use tracing::debug;
use std::path::PathBuf;
use tokio::fs;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub struct EndToEndTest {
    pub target_dir: String,
    pub query: String,
    pub truth: Vec<SearchResult>,
}

fn setup_logging() {
    // console layer (for tokio-console)
    let console_layer = console_subscriber::spawn();

    // stdout log layer
    let fmt_layer = fmt::layer().with_target(false).with_line_number(true);

    // enable turning logs on/off via RUST_LOG
    let filter = EnvFilter::from_default_env().add_directive("debug".parse().unwrap()); // default to debug level

    // build final subscriber
    tracing_subscriber::registry()
        .with(console_layer) // tokio-console data
        .with(fmt_layer) // normal stdout logs
        .with(filter) // controls log levels
        .init();
}

impl EndToEndTest {
    pub async fn execute(&self) {
        setup_logging();
        // 1. Index the target directory
        let index_output_dir = format!(
            "./test_output/{}.{}",
            chrono::Utc::now().format("%Y%m%d_%H%M%S"),
            rand::random::<u16>()
        );
        debug!("Building index at: {}", &index_output_dir);
        {
            fs::create_dir_all(&index_output_dir)
                .await
                .expect("Failed to create index output directory");
            let index_args = IndexArgs {
                target_dir: self.target_dir.clone(),
                includes: vec!["*".to_string()],
                excludes: vec![],
                read_chunk_size: 1024 * 1024,
                flush_threshold: 10 * 1024 * 1024,
                channel_capacity: 10,
                output_dir: index_output_dir.clone(),
                workers: 4,
                use_glob_cache: false,
            };

            let (sender, mut recvr) = tokio::sync::mpsc::channel(10);
            let task = tokio::spawn(index_directory(index_args, sender));
            let _drain =
                tokio::spawn(async move { while let Some(_progress) = recvr.recv().await {} });
            task.await.expect("").expect("");
        }

        // 2. Find all index files in output_dir
        let mut index_files = Vec::new();
        let mut entries = fs::read_dir(&index_output_dir).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            let path = entry.path();
            if path.is_file() {
                index_files.push(path);
            }
        }

        // 3. Search for the query
        let search_args = SearchArgs {
            index_files,
            workers: 4,
        };
        let (sender, mut recvr) = tokio::sync::mpsc::channel(10);
        let task = tokio::spawn(search_in_index_files(
            search_args,
            self.query.clone().into_bytes(),
            sender,
        ));
        let _drain = tokio::spawn(async move { while let Some(_progress) = recvr.recv().await {} });
        let search_result: Vec<SearchResult> = task.await.expect("").expect("");
        assert_eq!(search_result, self.truth);
    }
}
