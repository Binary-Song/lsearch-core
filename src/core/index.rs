use super::error::Error;
use super::glob::{glob, CompressedTree, GlobArgs};
use crate::core::io::{read_index_result, write_index_result};
use crate::core::progress::Progress;
pub use crate::prelude::*;
use futures::stream::{self, StreamExt};
pub use index_map::IndexMap;
use itertools::Itertools;
use std::arch::x86_64;
use std::collections::HashMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::{fs::File, io::AsyncReadExt};
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
    args: &IndexArgs,
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

async fn index_file(
    glob: &CompressedTree,
    args: &IndexArgs,
    file_index: usize,
) -> Result<(IndexMap, CompressedTree), Error> {
    let mut dep_tree = CompressedTree::new_empty();
    let path = glob.get_path_and_record_dependency(file_index, Some(&mut dep_tree))?;
    if !path.is_file() {
        return Err("Cannot index a non-file path."
            .to_string()
            .into_error(dbg_loc!()));
    }
    let file = File::open(&path).await;
    let mut file = file.map_err(|e| Error::CannotOpen {
        file_index: file_index,
        inner_err: e,
    })?;

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
            .map_err(|e| Error::CannotRead { inner_err: e })?;

        if bytes_read == 0 {
            // EOF
            return Ok((result, dep_tree));
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

pub async fn index_directory(
    args: IndexArgs,
    sender: tokio::sync::mpsc::Sender<Progress>,
) -> Result<(), Error> {
    // ----------------------------
    //             Prepare
    // ----------------------------
    println!("{:?}", &args.output_dir);
    // Create output directory if it doesn't exist
    tokio::fs::create_dir_all(&args.output_dir)
        .await
        .map_err(|e| Error::CannotWrite { inner_err: e })?;

    let args1 = args.clone();
    // ----------------------------
    //            Glob
    // ----------------------------
    let glob_cache_path = Path::new(&args1.output_dir).join("glob_cache.json");
    let glob_cache_file = File::open(glob_cache_path).await;
    let compressed_tree = match glob_cache_file {
        Ok(mut glob_cache_file) => {
            let r = read_index_result(Pin::new(&mut glob_cache_file)).await?;
            r.tree
        }
        Err(_) => {
            let glob_args = GlobArgs {
                target_dir: args1.target_dir,
                includes: args1.includes,
                excludes: args1.excludes,
            };
            glob(&glob_args, sender.clone()).await?
        }
    };

    // write glob cache
    let glob_cache_path = Path::new(&args.output_dir).join("glob_cache.json");
    let mut glob_cache_file = File::create(glob_cache_path)
        .await
        .map_err(|e| Error::CannotWrite { inner_err: e })?;
    write_index_result(
        Index {
            map: IndexMap::new(),
            tree: compressed_tree.clone(),
        },
        &mut glob_cache_file,
    )
    .await?;
    sender.send(Progress::GlobDone).await;

    // ----------------------------
    //      Task Creation
    // ----------------------------
    let glob = Arc::new(compressed_tree);
    let args: Arc<IndexArgs> = Arc::new(args);
    let total_files_to_index = glob.get_tree_len();
    // for each file globbed, create a worker task to index it
    // Create channel for worker results
    let (worker_sender, mut worker_recvr) = tokio::sync::mpsc::channel::<_>(args.channel_capacity);
    let mut worker_tasks = glob
        .get_tree_iter()
        .enumerate()
        .map(|(file_index, _entry)| {
            // below are member variables moved into async block
            let file_index = file_index;
            let glob = glob.clone();
            let args = args.clone();
            let worker_sender = worker_sender.clone();
            async move {
                let r = index_file(glob.as_ref(), args.as_ref(), file_index).await;
                worker_sender.send(r).await;
            }
        });

    // ----------------------------
    //          Task Awaiting
    // ----------------------------
    // keep awaiting any finished worker task
    // until all worker tasks are done

    async fn consumer(
        total_files_to_index: usize,
        mut worker_recvr: tokio::sync::mpsc::Receiver<Result<(IndexMap, CompressedTree), Error>>,
        global_sender: tokio::sync::mpsc::Sender<Progress>,
        args: Arc<IndexArgs>,
    ) -> Result<(), Error> {
        let mut buffer_index = IndexMap::new();
        let mut buffer_tree = CompressedTree::new_empty();
        let mut files_indexed = 0;
        let mut index_files_written = 0;
        while let Some(worker_result) = worker_recvr.recv().await {
            files_indexed += 1;
            match worker_result {
                Ok((index, tree)) => {
                    buffer_index.merge(index);
                    buffer_tree.merge(tree);
                    global_sender
                        .send(Progress::IndexAdded {
                            finished_entries: files_indexed,
                            total_entries: total_files_to_index,
                        })
                        .await;
                }
                Err(e) => {
                    continue;
                }
            };

            if buffer_index.estimated_size() >= args.break_size {
                // Write buffer to file
                let output_filename = format!("index_{}.json", index_files_written);
                let output_path = Path::new(&args.output_dir).join(output_filename);
                let mut output_file = File::create(output_path.clone())
                    .await
                    .map_err(|e| Error::CannotWrite { inner_err: e })?;
                write_index_result(
                    Index {
                        map: buffer_index,
                        tree: buffer_tree,
                    },
                    &mut output_file,
                )
                .await?;
                global_sender
                    .send(Progress::IndexWritten {
                        output_path: output_path.clone(),
                    })
                    .await;
                buffer_index = IndexMap::new();
                buffer_tree = CompressedTree::new_empty();
                index_files_written += 1;
            }
        }

        let output_filename = format!("index_{}.json", index_files_written);
        let output_path = Path::new(&args.output_dir).join(output_filename);
        let mut output_file = File::create(output_path.clone())
            .await
            .map_err(|e| Error::CannotWrite { inner_err: e })?;
        write_index_result(
            Index {
                map: buffer_index,
                tree: buffer_tree,
            },
            &mut output_file,
        )
        .await?;
        global_sender
            .send(Progress::IndexWritten {
                output_path: output_path.clone(),
            })
            .await;
        Ok(())
    }

    let consumer = tokio::spawn(consumer(
        total_files_to_index,
        worker_recvr,
        sender,
        args.clone(),
    ));

    let mut join_set = JoinSet::new();
    let mut count = 0;
    loop {
        match worker_tasks.next() {
            Some(t) => {
                join_set.spawn(t);
                count += 1;
            }
            None => {
                break {
                    join_set.join_all();
                }
            }
        }
        if count >= args.workers {
            // run the remaining task until done
            break loop {
                let join_result = join_set.join_next().await;
                match join_result {
                    // we got a finished task
                    Some(task_res) =>
                    // try to get another task to spawn
                    {
                        match worker_tasks.next() {
                            // we have another task to spawn
                            Some(t) => {
                                // spawn
                                join_set.spawn(t);
                            }
                            // we dont have any more tasks to spawn - keep draining
                            None => {
                                join_set.join_all();
                                break;
                            }
                        }
                    }
                    // we have drained all tasks
                    None => {
                        break;
                    }
                }
            };
        }
    }
    consumer.await;
    return Ok(());
}
