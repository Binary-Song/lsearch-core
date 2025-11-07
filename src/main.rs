use std::sync::Arc;
use std::{collections::HashMap, path::Path};
use tokio::task::{JoinError, JoinSet};
use tokio::{fs::File, io::AsyncReadExt, task::futures};
mod alloc;
mod error;
mod glob;
use crate::error::DisplayableError;
use crate::glob::CompressedTree;
use error::Error;
pub struct IndexArgs {
    glob_args: glob::GlobArgs,
    read_chunk_size: usize,
    gram_size: usize,
}

type FileId = usize;
type FileOffset = usize;

struct Offset {
    file_id: FileId,
    offset: FileOffset,
}

struct GramToOffsets {
    map: HashMap<Vec<u8>, Vec<Offset>>,
}

impl GramToOffsets {
    fn new() -> Self {
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
    gram_to_offsets: GramToOffsets,
    compressed_tree: glob::CompressedTree,
}

impl IndexResult {
    fn new(glob_result: glob::CompressedTree) -> Self {
        IndexResult {
            gram_to_offsets: GramToOffsets::new(),
            compressed_tree: glob_result,
        }
    }
}

impl DisplayableError for JoinError {
    fn display(&self) -> String {
        format!("{}", self)
    }
}

/// Index a chunk of data by sliding a window over it.
/// Like this:
///
/// ```text
/// datadatadatadatadatadatadatadatadatadatadatadatadata
/// [window] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> [window]
/// ```
pub async fn index_chunk(
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
        file_id_to_offsets.insert(
            file_id,
            Offset {
                file_id,
                offset: begin_offset + i,
            },
        );
    }
    return Ok(());
}

pub async fn index_file(
    glob: &CompressedTree,
    args: &IndexArgs,
    file_index: usize,
) -> Result<GramToOffsets, Error> {
    let path = glob.get_path(file_index);
    let file = File::open(&path).await;
    let mut file = file.map_err(|e| Error {
        message: format!("Failed to open {}", path.display()),
        inner: Some(Box::new(e)),
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
            .map_err(|e| Error::new(format!("Failed to read from {}", path.display()), e))?;

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

pub async fn index(args: IndexArgs) -> Result<(), Error> {
    let glob = match glob::glob(&args.glob_args) {
        Ok(r) => r,
        Err(e) => {
            return Err(Error {
                message: "Failed to glob".to_string(),
                inner: Some(Box::new(e)),
            });
        }
    };
    let glob = Arc::new(glob);
    let args = Arc::new(args);
    let tasks = glob.tree.iter().enumerate().map(|(file_index, _entry)| {
        let file_index = file_index;
        let glob = glob.clone();
        let args = args.clone();
        async move { index_file(glob.as_ref(), args.as_ref(), file_index).await }
    });
    let mut set = JoinSet::new();
    for task in tasks {
        set.spawn(task);
    }

    let mut final_res = GramToOffsets::new();
    // loop until all tasks are done
    while let Some(res) = set.join_next().await {
        let res = match res {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                return Err(e);
            }
            Err(e) => {
                return Err(Error {
                    message: "Task panicked".to_string(),
                    inner: Some(Box::new(e)),
                });
            }
        };
        final_res.merge(res);
    }

    return Ok(());
}

pub async fn search() {}

#[tokio::main]
async fn main() {}
