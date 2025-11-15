use super::glob::{glob, CompressedTree, GlobArgs};
use crate::core::io::{read_index_result, write_index_result};
use crate::core::progress::Progress;
use crate::prelude::*;
use futures::stream::{self, StreamExt};
use std::collections::HashMap;
use std::fmt::format;
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

#[derive(Debug, Clone)]
pub struct IndexArgs {
    pub target_dir: String,
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
    pub read_chunk_size: usize,
    pub break_size: usize,
    pub channel_capacity: usize,
    pub workers: usize,
    pub use_glob_cache: bool,
    pub output_dir: String,
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
    for i in 0..(chunk.len() - GRAM_SIZE + 1) {
        let gram = &chunk[i..i + GRAM_SIZE];
        // insert fileid and fileoffset into res
        res.insert(
            gram.try_into().unwrap(),
            Offset {
                file_id,
                offset: begin_offset + i,
            },
        );
    }
    return Ok(());
}

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
    let glob_cache_path = Path::new(&args.output_dir).join("glob_cache.json");
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
    let glob_cache_path = Path::new(&args.output_dir).join("glob_cache.json");
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
    file_index: usize,
    buffer_index: IndexMap,
    buffer_tree: CompressedTree,
    output_dir: PathBuf,
    sender: Sender<Progress>,
) -> Result<(), Error> {
    // Write buffer to file
    let output_filename = format!("index_{}.json", file_index);
    let output_path = output_dir.join(output_filename);
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

async fn consumer(
    total_files_to_index: usize,
    mut worker_recvr: Receiver<Result<IndexFileResult, Error>>,
    sender: Sender<Progress>,
    args: Arc<IndexArgs>,
) -> Result<(), Error> {
    let mut buffer_index = IndexMap::new();
    let mut buffer_tree = CompressedTree::new_empty();
    let mut files_indexed = 0;
    let mut index_files_written = 0;
    let mut join_set = JoinSet::new();
    let output_dir = PathBuf::try_from(&args.output_dir).map_error(dbg_loc!())?;
    while let Some(worker_result) = worker_recvr.recv().await {
        files_indexed += 1;
        match worker_result {
            Ok(IndexFileResult::Ok(index, tree)) => {
                buffer_index.merge(index);
                buffer_tree.merge(tree);
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

        if buffer_index.estimated_size() >= args.break_size {
            join_set.spawn(flush(
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
        index_files_written,
        buffer_index,
        buffer_tree,
        output_dir.clone(),
        sender.clone(),
    ));
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
    let compressed_tree = execute_glob(args.clone(), sender.clone()).await?;
    let compressed_tree: Arc<CompressedTree> = Arc::new(compressed_tree);
    let args: Arc<IndexArgs> = Arc::new(args);
    let total_files_to_index = compressed_tree.get_tree_len();
    let (worker_sender, worker_recvr) = channel::<_>(args.channel_capacity);
    // a lazy iterator that produces indexing tasks
    let task_iter = IndexTaskProducer {
        current_index: 0,
        tree: compressed_tree.clone(),
        args: args.clone(),
        worker_sender: worker_sender.clone(),
    };
    let consumer = tokio::spawn(consumer(
        total_files_to_index,
        worker_recvr,
        sender.clone(),
        args.clone(),
    ));
    // spawn indexing tasks while limiting concurrency: new tasks are spawned as previous tasks complete
    let mut result_iter = stream::iter(task_iter)
        .map(|index_task| tokio::spawn(index_task.task))
        .buffer_unordered(args.workers);

    // stream the index tasks and send results
    while let Some(result) = result_iter.next().await {
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
    consumer
        .await
        .map_error(dbg_loc!())?
        .map_error(dbg_loc!())?;
    return Ok(());     
}
