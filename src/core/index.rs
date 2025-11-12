use super::error::Error;
use super::glob::{glob, CompressedTree, GlobArgs};
use crate::core::io::write_index_result;
use crate::core::progress::Progress;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio::{fs::File, io::AsyncReadExt};

#[derive(Debug, Clone)]
pub struct IndexArgs {
    pub target_dir: String,
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
    pub read_chunk_size: usize,
    pub gram_size: usize,
    pub channel_capacity: usize,
    pub output_path: String,
}

type FileId = usize;
type FileOffset = usize;

pub struct Offset {
    pub file_id: FileId,
    pub offset: FileOffset,
}

pub struct GramToOffsets {
    pub map: HashMap<Vec<u8>, Vec<Offset>>,
}

impl GramToOffsets {
    pub fn new() -> Self {
        GramToOffsets {
            map: HashMap::new(),
        }
    }
    fn merge(&mut self, other: GramToOffsets) {
        for (gram, offsets) in other.map {
            let entry = self.map.entry(gram).or_default();
            entry.extend(offsets);
        }
    }
}

pub struct IndexResult {
    pub gram_to_offsets: GramToOffsets,
    pub compressed_tree: CompressedTree,
}

impl IndexResult {
    fn new(glob_result: CompressedTree) -> Self {
        IndexResult {
            gram_to_offsets: GramToOffsets::new(),
            compressed_tree: glob_result,
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
    res: &mut GramToOffsets,
) -> Result<(), Error> {
    if chunk.len() < args.gram_size {
        return Ok(());
    }
    for i in 0..(chunk.len() - args.gram_size + 1) {
        let gram = &chunk[i..i + args.gram_size];
        // insert fileid and fileoffset into res
        let file_id_to_offsets = res.map.entry(gram.to_vec()).or_default();
        file_id_to_offsets.push(Offset {
            file_id,
            offset: begin_offset + i,
        });
    }
    return Ok(());
}

async fn index_file(
    glob: &CompressedTree,
    args: &IndexArgs,
    file_index: usize,
) -> Result<GramToOffsets, Error> {
    let path = glob.get_path(file_index);
    if !path.is_file() {
        return Ok(GramToOffsets::new());
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
    let padding_size = args.gram_size - 1;
    let mut chunk = vec![0u8; padding_size + args.read_chunk_size];
    let mut current_offset = 0;
    let mut result = GramToOffsets {
        map: HashMap::new(),
    };

    loop {
        // read the next chunk
        let bytes_read = file
            .read(&mut chunk[padding_size..])
            .await
            .map_err(|e| Error::CannotRead { inner_err: e })?;

        if bytes_read == 0 {
            // EOF
            return Ok(result);
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
    yielder: tokio::sync::mpsc::Sender<Progress>,
) -> Result<(), Error> {
    // ----------------------------
    // Prepare
    // ----------------------------
    // open the output, aka the index file
    let mut output_file = tokio::fs::File::create(Path::new(&args.output_path))
        .await
        .map_err(|e| {
            return Error::CannotWrite { inner_err: e };
        })?;

    let args1 = args.clone();
    // ----------------------------
    // Glob
    // ----------------------------
    let glob_args = GlobArgs {
        target_dir: args1.target_dir,
        includes: args1.includes,
        excludes: args1.excludes,
    };
    let compressed_tree = glob(&glob_args, yielder.clone()).await?;
    // ----------------------------
    // Index
    // ----------------------------
    let glob = Arc::new(compressed_tree);
    let args: Arc<IndexArgs> = Arc::new(args);
    // create a bunch of tasks
    let worker_tasks = glob.tree.iter().enumerate().map(|(file_index, _entry)| {
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

    let mut final_res = GramToOffsets::new();
    let mut indexed = 0;
    let total = glob.tree.len();
    // keep awaiting for worker results
    // loop until all worker tasks are done
    while let Some(res) = join_set.join_next().await {
        indexed += 1;
        match res {
            Ok(Ok(r)) => {
                final_res.merge(r);
                yielder
                    .send(Progress::IndexAdded {
                        finished_entries: indexed,
                        total_entries: total,
                    })
                    .await;
                continue;
            }
            Ok(Err(e)) => {
                continue;
            }
            Err(e) => {
                return Err(Error::TaskDiedWithJoinError { inner: e });
            }
        };
    }
    yielder.send(Progress::Writing).await;
    // write index to file
    let tree = Arc::try_unwrap(glob).map_err(|e| Error::LogicalError {
                    message: "Unable to unwrap the globbed result. This should not happen because all tasks referencing it should be done at this point."
                        .to_string(),
                })?;
    write_index_result(
        IndexResult {
            gram_to_offsets: final_res,
            compressed_tree: tree,
        },
        &mut output_file,
    )
    .await?;
    return Ok(());
}
