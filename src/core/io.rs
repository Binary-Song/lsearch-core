use super::{
    error::Error,
    glob::{CompressedTree, TreeEntry},
    index::{GramToOffsets, IndexResult, Offset},
};
use encoding_rs::WINDOWS_1252 as THE_ENCODING;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, pin::Pin};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
    index: HashMap<String, Vec<IOOffset>>,
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
                    let (gram_string, _, _) = THE_ENCODING.decode(&gram);
                    let io_offsets = offsets
                        .into_iter()
                        .map(|offset| IOOffset(offset.file_id, offset.offset))
                        .collect();
                    (gram_string.to_string(), io_offsets)
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
            let gram_bytes = THE_ENCODING.encode(&gram).0.to_vec();
            gram_to_offsets.map.insert(gram_bytes, offsets);
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
    let bytes = serde_json::to_string(&io_index_result).map_err(|e| {
        return Error::JsonError {
            inner: e,
            reason: "s5",
        };
    })?;
    out.write_all(bytes.as_bytes()).await.map_err(|e| {
        return Error::CannotWrite { inner_err: e };
    })?;
    Ok(())
}

pub async fn read_index_result(
    mut input: Pin<&mut impl AsyncReadExt>,
) -> Result<IndexResult, Error> {
    let mut contents = Vec::new();
    input
        .read_to_end(&mut contents)
        .await
        .map_err(|e| Error::CannotRead { inner_err: e })?;
    let str = String::from_utf8(contents).map_err(|e| Error::DecodeUtf8Error { error: e })?;
    let io_index_result: IOIndexResult = serde_json::from_str(&str).map_err(|e| {
        return Error::JsonError {
            inner: e,
            reason: "s6",
        };
    })?;
    Ok(io_index_result.into())
}
