use super::error::Error;
use super::glob::{glob, CompressedTree, GlobArgs};
use crate::core::io::write_index_result;
use crate::core::progress::Progress;
pub use crate::prelude::*;
pub use index_map::IndexMap;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
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
    pub output_dir: String,
}

type FileId = usize;
type FileOffset = usize;

pub struct Offset {
    pub file_id: FileId,
    pub offset: FileOffset,
}

mod index_map {
    use super::*;

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
    let glob_args = GlobArgs {
        target_dir: args1.target_dir,
        includes: args1.includes,
        excludes: args1.excludes,
    };
    let compressed_tree = glob(&glob_args, sender.clone()).await?;
    sender
        .send(Progress::GlobDone)
        .await;
    
    // ----------------------------
    //      Task Creation
    // ----------------------------
    let glob = Arc::new(compressed_tree);
    let args: Arc<IndexArgs> = Arc::new(args);
    // for each file globbed, create a worker task to index it
    let worker_tasks = glob
        .get_tree_iter()
        .enumerate()
        .map(|(file_index, _entry)| {
            // below are member variables moved into async block
            let file_index = file_index;
            let glob = glob.clone();
            let args = args.clone();
            async move { index_file(glob.as_ref(), args.as_ref(), file_index).await }
        });
    let mut join_set = JoinSet::new();
    for task in worker_tasks {
        join_set.spawn(task);
    }

    let mut index_res = IndexMap::new();
    let mut part_tree_res = CompressedTree::new_empty();
    let mut indexed = 0;
    let mut index_file = 0;
    let total = glob.get_tree_len();
    // ----------------------------
    //          Task Awaiting
    // ----------------------------
    // keep awaiting any finished worker task
    // until all worker tasks are done

    while let Some(worker_result) = join_set.join_next().await {
        indexed += 1;
        match worker_result {
            Ok(Ok((index, part_tree))) => {
                index_res.merge(index);
                part_tree_res.merge(part_tree);
                sender
                    .send(Progress::IndexAdded {
                        finished_entries: indexed,
                        total_entries: total,
                    })
                    .await;
            }
            Ok(Err(e)) => {
                continue;
            }
            Err(e) => {
                return Err(Error::TaskDiedWithJoinError { inner: e });
            }
        };

        if index_res.estimated_size() >= args.break_size {
            let output_filename = format!("index_{}.json", index_file);
            let output_path = Path::new(&args.output_dir).join(output_filename);
            let mut output_file = File::create(output_path.clone())
                .await
                .map_err(|e| Error::CannotWrite { inner_err: e })?;
            write_index_result(
                Index {
                    map: index_res,
                    tree: part_tree_res,
                },
                &mut output_file,
            )
            .await?;
            sender
                .send(Progress::IndexWritten {
                    output_path: output_path.clone(),
                })
                .await;
            index_res = IndexMap::new();
            part_tree_res = CompressedTree::new_empty();
            index_file += 1;
        }
    }

    let output_filename = format!("index_{}.json", index_file);
    let output_path = Path::new(&args.output_dir).join(output_filename);
    let mut output_file = File::create(output_path)
        .await
        .map_err(|e| Error::CannotWrite { inner_err: e })?;
    write_index_result(
        Index {
            map: index_res,
            tree: part_tree_res,
        },
        &mut output_file,
    )
    .await?;
    return Ok(());
}
