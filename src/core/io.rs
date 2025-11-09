use super::{
    error::Error,
    glob::{CompressedTree, TreeEntry},
    index::{GramToOffsets, IndexResult, Offset},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::io::AsyncWriteExt;

#[derive(Serialize, Deserialize, Debug)]
struct IOTreeEntry(
    /// parent_index
    usize,
    /// string_index
    usize,
);

#[derive(Serialize, Deserialize, Debug)]
struct IOOffset(
    /// file_index
    usize,
    /// offset
    usize,
);

#[derive(Serialize, Deserialize, Debug)]
struct IOIndexResult {
    strings: Vec<String>,
    tree: Vec<IOTreeEntry>,
    index: HashMap<Vec<u8>, Vec<IOOffset>>,
}

impl From<IndexResult> for IOIndexResult {
    fn from(src: IndexResult) -> Self {
        IOIndexResult {
            strings: src.compressed_tree.strings,
            tree: src
                .compressed_tree
                .tree
                .into_iter()
                .map(|entry| IOTreeEntry(entry.parent_index, entry.string_index))
                .collect(),
            index: src
                .gram_to_offsets
                .map
                .into_iter()
                .map(|(gram, offsets)| {
                    let io_offsets = offsets
                        .into_iter()
                        .map(|offset| IOOffset(offset.file_id, offset.offset))
                        .collect();
                    (gram, io_offsets)
                })
                .collect(),
        }
    }
}

impl Into<IndexResult> for IOIndexResult {
    fn into(self) -> IndexResult {
        let compressed_tree = CompressedTree {
            strings: self.strings,
            tree: self
                .tree
                .into_iter()
                .map(|entry| TreeEntry {
                    parent_index: entry.0,
                    string_index: entry.1,
                })
                .collect(),
        };
        let mut gram_to_offsets = GramToOffsets::new();
        for (gram, io_offsets) in self.index {
            let offsets = io_offsets
                .into_iter()
                .map(|io_offset| Offset {
                    file_id: io_offset.0,
                    offset: io_offset.1,
                })
                .collect();
            gram_to_offsets.map.insert(gram, offsets);
        }
        IndexResult {
            gram_to_offsets,
            compressed_tree,
        }
    }
}

pub async fn write_index_result(
    index_result: IndexResult,
    out: &mut (impl AsyncWriteExt + std::marker::Unpin),
) -> Result<(), Error> {
    let io_index_result: IOIndexResult = index_result.into();
    let bytes = serde_json::to_vec(&io_index_result).map_err(|e| {
        return Error::SerdeJsonFailed { inner: e };
    })?;
    out.write_all(&bytes).await.map_err(|e| {
        return Error::CannotWrite { inner_err: e };
    })?;
    Ok(())
}
