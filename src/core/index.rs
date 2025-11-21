use super::glob::{glob, CompressedTree, GlobArgs};
use crate::core::io::{read_index_result, write_index_result};
use crate::core::progress::Progress;
use crate::prelude::*;
use futures::stream::{self, StreamExt};
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinSet;
use tokio::{fs::File, io::AsyncReadExt};

pub use index_map::IndexMap;
pub const GRAM_SIZE: usize = 4;
pub type Gram = [u8; GRAM_SIZE];

#[derive(Debug, Clone, Default)]
pub struct IndexArgs {
    pub target_dir: String,
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
    pub read_chunk_size: usize,
    pub flush_threshold: usize,
    pub channel_capacity: usize,
    pub workers: usize,
    pub use_glob_cache: bool,
    pub output_dir: String,
    pub num_groups: usize, // Number of gram groups for stratification
    pub sampling_rate: f64, // e.g., 0.01 for 1%
}

/// Represents a gram range [start, end) for a group
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GramRange {
    pub start: Vec<u8>, // Start of gram range (inclusive)
    pub end: Vec<u8>,   // End of gram range (exclusive)
}

impl GramRange {
    pub fn contains(&self, gram: &Gram) -> bool {
        let gram_slice = gram.as_slice();
        gram_slice >= self.start.as_slice() && gram_slice < self.end.as_slice()
    }
}

/// Metadata about gram stratification
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StratificationMetadata {
    pub groups: Vec<GramRange>,
}

type FileId = usize;
type FileOffset = usize;

#[derive(Debug)]
pub struct Offset {
    pub file_id: FileId,
    pub offset: FileOffset,
}

mod index_map {
    use super::*;

    #[derive(Debug)]
    pub struct IndexMap {
        /// mapping from grams (little pieces of substrings) to list of offsets where it appears
        map: HashMap<Gram, Vec<Offset>>,
        estimated_size: usize,
    }

    impl IndexMap {
        pub fn new() -> Self {
            IndexMap {
                map: HashMap::new(),
                estimated_size: 0,
            }
        }
        pub fn into_iter(self) -> impl Iterator<Item = (Gram, Vec<Offset>)> {
            self.map.into_iter()
        }
        pub fn estimated_size(&self) -> usize {
            self.estimated_size
        }
        pub fn merge(&mut self, other: IndexMap) {
            for (gram, offsets) in other.map {
                for offset in offsets {
                    self.insert(gram.clone(), offset);
                }
            }
        }
        pub fn get(&self, gram: &Gram) -> Option<&Vec<Offset>> {
            self.map.get(gram)
        }
        pub fn insert(&mut self, gram: Gram, offset: Offset) {
            use std::collections::hash_map::Entry;
            let gram_size = gram.len();
            let offset_size = std::mem::size_of::<Offset>();
            match self.map.entry(gram) {
                Entry::Vacant(e) => {
                    self.estimated_size += gram_size + offset_size;
                    let offsets = e.insert(Vec::new());
                    offsets.push(offset);
                }
                Entry::Occupied(mut e) => {
                    self.estimated_size += offset_size;
                    e.get_mut().push(offset);
                }
            }
        }
    }
}
pub struct Index {
    pub map: IndexMap,
    pub tree: CompressedTree,
}

impl Index {
    pub fn new() -> Self {
        Index {
            map: IndexMap::new(),
            tree: CompressedTree::new_empty(),
        }
    }
}

/// Index a chunk of data by sliding a window over it.
/// Like this:
///
/// ```text
/// datadatadatadatadatadatadatadatadatadatadatadatadata
/// [window] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> [window]
/// ```
async fn index_chunk(
    chunk: &[u8],
    begin_offset: FileOffset,
    file_id: FileId,
    _args: &IndexArgs,
    res: &mut IndexMap,
) -> Result<(), Error> {
    if chunk.len() < GRAM_SIZE {
        return Ok(());
    }
    let padding_size = GRAM_SIZE - 1;
    for i in 0..(chunk.len() - GRAM_SIZE + 1) {
        let gram = &chunk[i..i + GRAM_SIZE];
        // insert fileid and fileoffset into res
        // Subtract padding_size from the offset to get the actual file position
        // The first `padding_size` bytes in the chunk are padding from the previous chunk
        let actual_offset = if i >= padding_size {
            begin_offset + (i - padding_size)
        } else {
            // This gram includes padding bytes, so it starts before begin_offset
            begin_offset.saturating_sub(padding_size - i)
        };
        res.insert(
            gram.try_into().unwrap(),
            Offset {
                file_id,
                offset: actual_offset,
            },
        );
    }
    return Ok(());
}

#[derive(Debug)]
enum IndexFileResult {
    Ok(IndexMap, CompressedTree),
    Skipped,
}

async fn index_file(
    glob: &CompressedTree,
    args: &IndexArgs,
    file_index: usize,
) -> Result<IndexFileResult, Error> {
    let mut dep_tree = CompressedTree::new_empty();
    let path = glob.get_path_and_record_dependency(file_index, Some(&mut dep_tree))?;
    if !path.is_file() {
        if path.is_dir() {
            return Ok(IndexFileResult::Skipped);
        }
        return Err(
            format!("Cannot index non-file path: {}", path.to_string_lossy())
                .to_string()
                .into_error(dbg_loc!()),
        );
    }
    let file = File::open(&path).await;
    let mut file = file.map_error(dbg_loc!())?;

    // assuming gram_size == 4
    // first 3 bytes are padding from last chunk (initially zeros)
    // we read the current chunk into the 4th byte onwards
    // this way we can always have a full gram when we process the chunk
    let padding_size = GRAM_SIZE - 1;
    let mut chunk = vec![0u8; padding_size + args.read_chunk_size];
    let mut current_offset = 0;
    let mut result = IndexMap::new();

    loop {
        // read the next chunk
        let bytes_read = file
            .read(&mut chunk[padding_size..])
            .await
            .map_error(dbg_loc!())?;

        if bytes_read == 0 {
            // EOF
            return Ok(IndexFileResult::Ok(result, dep_tree));
        }
        // now the chunk looks like this:
        // 0       padding_size       padding_size+bytes_read
        // |padding|       data       |
        index_chunk(
            &chunk[0..padding_size + bytes_read],
            current_offset,
            file_index,
            &args,
            &mut result,
        )
        .await?;
        // maintain the padding
        if bytes_read >= padding_size {
            // copy from ssss to dddd
            // 0    padding_size       padding_size+bytes_read
            // |dddd|              ssss|
            chunk.copy_within(bytes_read..bytes_read + padding_size, 0);
        } else {
            // if you read less than padding size, you need to shift it into the padding
            //
            // Before:
            //  0    p
            //  | sss|s|
            //         p+b
            // After:
            //  0    p
            //  |ssss| |
            //         p+b
            chunk.copy_within(bytes_read..padding_size + bytes_read, 0);
        }
        current_offset += bytes_read;
    }
}

pub enum BuildIndexProgress {
    /// A file was just added to the index.
    Updated { total: usize, indexed: usize },
    /// An error occurred while indexing a file, so the file was skipped.
    UpdatedErr(Error),
}

async fn execute_glob(
    args: IndexArgs,
    sender: tokio::sync::mpsc::Sender<Progress>,
) -> Result<CompressedTree, Error> {
    let glob_cache_path = Path::new(&args.output_dir).join("glob_cache.bin");
    let glob_cache_file = File::open(glob_cache_path.clone())
        .await
        .map_error(dbg_loc!());
    // get the glob result by either reading from cache or globbing
    let compressed_tree = {
        match (glob_cache_file, args.use_glob_cache) {
            (Ok(mut glob_cache_file), true) => {
                let index = read_index_result(Pin::new(&mut glob_cache_file))
                    .await
                    .map_error(dbg_loc!())?;
                index.tree
            }
            _ => {
                let glob_args = GlobArgs {
                    target_dir: PathBuf::from(args.target_dir),
                    includes: args.includes,
                    excludes: args.excludes,
                };
                glob(&glob_args, sender.clone())
                    .await
                    .map_error(dbg_loc!())?
            }
        }
    };
    // write glob cache
    let glob_cache_path = Path::new(&args.output_dir).join("glob_cache.bin");
    let mut glob_cache_file = File::create(glob_cache_path.clone())
        .await
        .map_err(|e| {
            format!(
                "Failed to create file {}: {}",
                glob_cache_path.to_string_lossy(),
                e
            )
        })
        .map_error(dbg_loc!())?;
    write_index_result(
        Index {
            map: IndexMap::new(),
            tree: compressed_tree.clone(),
        },
        &mut glob_cache_file,
    )
    .await?;
    sender
        .send(Progress::GlobDone)
        .await
        .map_err(|e| e.into_error(dbg_loc!()))?;
    Ok(compressed_tree)
}

/// Sample a subset of files to build gram distribution
async fn sample_files(
    tree: &CompressedTree,
    args: &IndexArgs,
    sample_indices: &[usize],
) -> Result<HashMap<Gram, usize>, Error> {
    let mut gram_counts: HashMap<Gram, usize> = HashMap::new();
    
    for &file_index in sample_indices {
        // Index this file and collect gram counts
        match index_file(tree, args, file_index).await? {
            IndexFileResult::Ok(index_map, _) => {
                for (gram, offsets) in index_map.into_iter() {
                    *gram_counts.entry(gram).or_insert(0) += offsets.len();
                }
            }
            IndexFileResult::Skipped => {}
        }
    }
    
    Ok(gram_counts)
}

/// Create stratification metadata by dividing grams into groups with equal occurrence distribution
fn create_stratification(
    gram_counts: HashMap<Gram, usize>,
    num_groups: usize,
) -> StratificationMetadata {
    if num_groups == 0 {
        panic!("num_groups must be > 0");
    }
    
    // Sort grams by their byte values
    let mut gram_data: Vec<(Gram, usize)> = gram_counts.into_iter().collect();
    gram_data.sort_by(|(a, _), (b, _)| a.cmp(b));
    
    // Calculate total occurrences and target per group
    let total_occurrences: usize = gram_data.iter().map(|(_, count)| count).sum();
    let target_per_group = total_occurrences / num_groups;
    
    let mut groups = Vec::new();
    let mut current_start = vec![0u8; GRAM_SIZE];
    let mut current_count = 0usize;
    
    let mut gram_index = 0;
    
    for group_idx in 0..num_groups {
        // Accumulate occurrences until we reach the target for this group
        while gram_index < gram_data.len() && current_count < target_per_group && group_idx < num_groups - 1 {
            current_count += gram_data[gram_index].1;
            gram_index += 1;
        }
        
        // Determine the end boundary for this group
        let end = if group_idx == num_groups - 1 {
            // Last group extends to maximum
            vec![0xFFu8; GRAM_SIZE]
        } else if gram_index < gram_data.len() {
            // Split at this gram - this gram goes in the next group
            gram_data[gram_index].0.to_vec()
        } else {
            // Ran out of data - extend to maximum
            vec![0xFFu8; GRAM_SIZE]
        };
        
        groups.push(GramRange {
            start: current_start.clone(),
            end: end.clone(),
        });
        
        // Next group starts where this one ends
        current_start = end;
        current_count = 0;
    }
    
    // If we have no data, create equal-sized ranges based on first byte
    if groups.is_empty() {
        // Divide the first byte space uniformly
        let bytes_per_group = 256 / num_groups;
        for i in 0..num_groups {
            let mut start = vec![0u8; GRAM_SIZE];
            let mut end = vec![0u8; GRAM_SIZE];
            
            start[0] = (i * bytes_per_group) as u8;
            
            if i == num_groups - 1 {
                // Last group extends to maximum
                end = vec![0xFFu8; GRAM_SIZE];
            } else {
                end[0] = ((i + 1) * bytes_per_group) as u8;
            }
            
            groups.push(GramRange { start, end });
        }
    }
    
    StratificationMetadata { groups }
}



/// Execute sampling phase and create stratification
async fn execute_sampling_and_stratification(
    tree: &CompressedTree,
    args: &IndexArgs,
    sender: &Sender<Progress>,
) -> Result<StratificationMetadata, Error> {
    let stratification_path = Path::new(&args.output_dir).join("stratification.json");
    
    // Try to load existing stratification
    if let Ok(file_content) = tokio::fs::read_to_string(&stratification_path).await {
        if let Ok(metadata) = serde_json::from_str::<StratificationMetadata>(&file_content) {
            debug!("Loaded existing stratification metadata");
            return Ok(metadata);
        }
    }
    
    debug!("Starting sampling phase");
    let total_files = tree.get_tree_len();
    let sample_size = ((total_files as f64 * args.sampling_rate).ceil() as usize).max(1);
    
    // Randomly select files for sampling
    use rand::prelude::*;
    let mut rng = rand::rngs::StdRng::seed_from_u64(42); // Use deterministic seed for reproducibility
    let all_indices: Vec<usize> = (0..total_files).collect();
    let sample_indices: Vec<usize> = all_indices
        .choose_multiple(&mut rng, sample_size)
        .copied()
        .collect();
    
    debug!("Sampling {} out of {} files", sample_size, total_files);
    
    // Sample files
    let gram_counts = sample_files(tree, args, &sample_indices).await?;
    debug!("Sampled {} unique grams", gram_counts.len());
    
    // Create stratification
    let metadata = create_stratification(gram_counts, args.num_groups);
    debug!("Created {} gram groups", metadata.groups.len());
    
    // Save stratification metadata
    let json = serde_json::to_string_pretty(&metadata).map_error(dbg_loc!())?;
    tokio::fs::write(&stratification_path, json).await.map_error(dbg_loc!())?;
    
    sender
        .send(Progress::StratificationDone)
        .await
        .map_error(dbg_loc!())?;
    
    Ok(metadata)
}

struct IndexTaskProducer {
    current_index: usize,
    tree: Arc<CompressedTree>,
    args: Arc<IndexArgs>,
    worker_sender: Sender<Result<IndexFileResult, Error>>,
}

struct IndexTask {
    task: Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'static>>,
}

impl Iterator for IndexTaskProducer {
    type Item = IndexTask;
    fn next(&mut self) -> Option<IndexTask> {
        let len = self.tree.get_tree_len();
        if self.current_index >= len {
            return None;
        }
        let tree = self.tree.clone();
        let args = self.args.clone();
        let file_index = self.current_index;
        let worker_sender = self.worker_sender.clone();
        let future = async move {
            let result = index_file(tree.as_ref(), args.as_ref(), file_index).await;
            worker_sender.send(result).await.map_error(dbg_loc!())?;
            Ok(())
        };
        self.current_index += 1;
        Some(IndexTask {
            task: Box::pin(future),
        })
    }
}

async fn flush(
    group_id: usize,
    file_index: usize,
    buffer_index: IndexMap,
    buffer_tree: CompressedTree,
    output_dir: PathBuf,
    sender: Sender<Progress>,
) -> Result<(), Error> {
    // Write buffer to file in group-specific directory
    let group_dir = output_dir.join(format!("group_{}", group_id));
    tokio::fs::create_dir_all(&group_dir).await.map_error(dbg_loc!())?;
    
    let output_filename = format!("index_{}.bin", file_index);
    let output_path = group_dir.join(output_filename);
    let mut output_file = File::create(output_path.clone())
        .await
        .map_error(dbg_loc!())?;
    write_index_result(
        Index {
            map: buffer_index,
            tree: buffer_tree,
        },
        &mut output_file,
    )
    .await
    .map_error(dbg_loc!())?;
    sender
        .send(Progress::IndexWritten {
            output_path: output_path.clone(),
        })
        .await
        .map_error(dbg_loc!())?;
    Ok(())
}

/// Helper to determine which group a gram belongs to
fn find_group_for_gram(gram: &Gram, stratification: &StratificationMetadata) -> usize {
    for (i, range) in stratification.groups.iter().enumerate() {
        if range.contains(gram) {
            return i;
        }
    }
    // Default to last group if not found
    stratification.groups.len() - 1
}

async fn consumer_stratified(
    total_files_to_index: usize,
    mut worker_recvr: Receiver<Result<IndexFileResult, Error>>,
    sender: Sender<Progress>,
    args: Arc<IndexArgs>,
    stratification: Arc<StratificationMetadata>,
) -> Result<(), Error> {
    debug!("Consumer started with stratification");
    
    // Create one buffer per group
    let num_groups = stratification.groups.len();
    let mut group_buffers: Vec<IndexMap> = (0..num_groups).map(|_| IndexMap::new()).collect();
    let mut group_trees: Vec<CompressedTree> = (0..num_groups).map(|_| CompressedTree::new_empty()).collect();
    let mut group_file_counters: Vec<usize> = vec![0; num_groups];
    
    let mut files_indexed = 0;
    let mut join_set = JoinSet::new();
    let output_dir = PathBuf::try_from(&args.output_dir).map_error(dbg_loc!())?;
    
    while let Some(worker_result) = worker_recvr.recv().await {
        debug!("Consumer: Worker result received, ok = {:?}", worker_result.is_ok());
        files_indexed += 1;
        
        match worker_result {
            Ok(IndexFileResult::Ok(index, tree)) => {
                // Route each gram to its corresponding group
                for (gram, offsets) in index.into_iter() {
                    let group_id = find_group_for_gram(&gram, &stratification);
                    for offset in offsets {
                        group_buffers[group_id].insert(gram.clone(), offset);
                    }
                }
                
                // Merge tree into all groups (file paths are needed by all groups)
                for group_tree in &mut group_trees {
                    group_tree.merge(tree.clone());
                }
                
                sender
                    .send(Progress::IndexAdded {
                        finished_entries: files_indexed,
                        total_entries: total_files_to_index,
                    })
                    .await
                    .map_error(dbg_loc!())?;
            }
            Ok(IndexFileResult::Skipped) => {}
            Err(e) => {
                sender
                    .send(Progress::ErrorOccurred {
                        message: format!("{e}"),
                    })
                    .await
                    .map_error(dbg_loc!())?;
                continue;
            }
        };

        // Check each group buffer and flush if needed
        for group_id in 0..num_groups {
            if group_buffers[group_id].estimated_size() >= args.flush_threshold {
                let buffer_index = std::mem::replace(&mut group_buffers[group_id], IndexMap::new());
                let buffer_tree = std::mem::replace(&mut group_trees[group_id], CompressedTree::new_empty());
                let file_index = group_file_counters[group_id];
                group_file_counters[group_id] += 1;
                
                join_set.spawn(flush(
                    group_id,
                    file_index,
                    buffer_index,
                    buffer_tree,
                    output_dir.clone(),
                    sender.clone(),
                ));
            }
        }
    }
    
    // Flush all remaining buffers
    for group_id in 0..num_groups {
        if group_buffers[group_id].estimated_size() > 0 {
            let buffer_index = std::mem::replace(&mut group_buffers[group_id], IndexMap::new());
            let buffer_tree = std::mem::replace(&mut group_trees[group_id], CompressedTree::new_empty());
            let file_index = group_file_counters[group_id];
            
            join_set.spawn(flush(
                group_id,
                file_index,
                buffer_index,
                buffer_tree,
                output_dir.clone(),
                sender.clone(),
            ));
        }
    }
    
    debug!("Consumer: Awaiting all flush tasks");
    join_set.join_all().await;
    Ok(())
}

async fn consumer(
    total_files_to_index: usize,
    mut worker_recvr: Receiver<Result<IndexFileResult, Error>>,
    sender: Sender<Progress>,
    args: Arc<IndexArgs>,
) -> Result<(), Error> {
    debug!("Consumer started");
    let mut buffer_index = IndexMap::new();
    let mut buffer_tree = CompressedTree::new_empty();
    let mut files_indexed = 0;
    let mut index_files_written = 0;
    let mut join_set = JoinSet::new();
    let output_dir = PathBuf::try_from(&args.output_dir).map_error(dbg_loc!())?;
    while let Some(worker_result) = worker_recvr.recv().await {
        debug!("Consumer: Worker result received, ok = {:?}", worker_result.is_ok());
        files_indexed += 1;
        match worker_result {
            Ok(IndexFileResult::Ok(index, tree)) => {
                buffer_index.merge(index);
                buffer_tree.merge(tree);
                debug!("Consumer: sending result");
                sender
                    .send(Progress::IndexAdded {
                        finished_entries: files_indexed,
                        total_entries: total_files_to_index,
                    })
                    .await
                    .map_error(dbg_loc!())?;
                debug!("Consumer: sending result done");
            }
            Ok(IndexFileResult::Skipped) => {}
            Err(e) => {
                sender
                    .send(Progress::ErrorOccurred {
                        message: format!("{e}"),
                    })
                    .await
                    .map_error(dbg_loc!())?;
                continue;
            }
        };

        if buffer_index.estimated_size() >= args.flush_threshold {
            join_set.spawn(flush(
                0, // group_id (old consumer uses single group)
                index_files_written,
                buffer_index,
                buffer_tree,
                output_dir.clone(),
                sender.clone(),
            ));
            index_files_written += 1;
            buffer_index = IndexMap::new();
            buffer_tree = CompressedTree::new_empty();
        }
    }
    join_set.spawn(flush(
        0, // group_id (old consumer uses single group)
        index_files_written,
        buffer_index,
        buffer_tree,
        output_dir.clone(),
        sender.clone(),
    ));
    debug!("Consumer: Awaiting all flush tasks");
    join_set.join_all().await;
    Ok(())
}

pub async fn index_directory(
    args: IndexArgs,
    sender: tokio::sync::mpsc::Sender<Progress>,
) -> Result<(), Error> {
    let target_dir = PathBuf::from(args.target_dir.clone());
    let output_dir = PathBuf::from(args.output_dir.clone());
    if !target_dir.exists() {
        return Err(format!(
            "Target dir {} does not exist.",
            target_dir.to_string_lossy()
        )
        .to_string()
        .into_error(dbg_loc!()));
    }
    if !target_dir.is_dir() {
        return Err(format!(
            "Target dir {} is not a directory.",
            target_dir.to_string_lossy()
        )
        .to_string()
        .into_error(dbg_loc!()));
    }
    if !output_dir.exists() {
        std::fs::create_dir(output_dir.clone())
            .map_err(|e| {
                format!(
                    "Failed to create output directory {}: {}",
                    output_dir.to_string_lossy(),
                    e
                )
            })
            .map_error(dbg_loc!())?;
    } else {
        if !target_dir.is_dir() {
            return Err(format!(
                "Output path {} is not a directory.",
                target_dir.to_string_lossy()
            )
            .to_string()
            .into_error(dbg_loc!()));
        }
    }
    // glob
    debug!("Glob directory: {}", &args.target_dir);
    let compressed_tree = execute_glob(args.clone(), sender.clone()).await?;
    debug!("Glob complete, tree size: {}", compressed_tree.get_tree_len());
    
    // Execute sampling and stratification
    let stratification = execute_sampling_and_stratification(&compressed_tree, &args, &sender).await?;
    let stratification = Arc::new(stratification);
    
    let compressed_tree: Arc<CompressedTree> = Arc::new(compressed_tree);
    let args: Arc<IndexArgs> = Arc::new(args);
    let total_files_to_index = compressed_tree.get_tree_len();
    let (worker_sender, worker_recvr) = channel::<_>(args.channel_capacity);
    // a lazy iterator that produces indexing tasks
    let task_iter = IndexTaskProducer {
        current_index: 0,
        tree: compressed_tree.clone(),
        args: args.clone(),
        worker_sender: worker_sender,
    };
    
    // Use stratified consumer if num_groups > 1
    let consumer = if args.num_groups > 1 {
        tokio::spawn(consumer_stratified(
            total_files_to_index,
            worker_recvr,
            sender.clone(),
            args.clone(),
            stratification.clone(),
        ))
    } else {
        tokio::spawn(consumer(
            total_files_to_index,
            worker_recvr,
            sender.clone(),
            args.clone(),
        ))
    };
    // spawn indexing tasks while limiting concurrency: new tasks are spawned as previous tasks complete
    let mut result_iter = stream::iter(task_iter)
        .map(|index_task| tokio::spawn(index_task.task))
        .buffer_unordered(args.workers);

    debug!("Awaiting indexing tasks");
    // stream the index tasks and send results.
    while let Some(result) = result_iter.next().await {
        debug!("Indexing task completed");
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                sender
                    .clone()
                    .send(Progress::ErrorOccurred {
                        message: format!("{e:?}"),
                    })
                    .await
                    .map_error(dbg_loc!())?;
            }
            Err(e) => {
                sender
                    .clone()
                    .send(Progress::ErrorOccurred {
                        message: format!("{e:?}"),
                    })
                    .await
                    .map_error(dbg_loc!())?;
            }
        }
    }
    drop(result_iter);
    debug!("Awaiting consumer");
    consumer
        .await
        .map_error(dbg_loc!())?
        .map_error(dbg_loc!())?;
    debug!("Index directory complete");
    return Ok(());
}
