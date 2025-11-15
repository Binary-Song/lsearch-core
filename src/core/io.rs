use super::{
    error::Error,
    glob::{CompressedTree, TreeEntry},
    index::{Index, IndexMap, Offset},
};
use crate::prelude::*;
use blake3::hash;
use encoding_rs::WINDOWS_1252 as THE_ENCODING;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, pin::Pin};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Serialize, Deserialize, Debug)]
struct IOTreeEntry {
    parent_id: usize,
    string_id: usize,
}

#[derive(Serialize, Deserialize, Debug)]
struct IOOffset {
    file_id: usize,
    offset: usize,
}

#[derive(Serialize, Deserialize, Debug)]
struct IOIndexResult {
    strings: HashMap<usize, String>,
    tree: HashMap<usize, IOTreeEntry>,
    index: HashMap<String, Vec<IOOffset>>,
}

impl From<Index> for IOIndexResult {
    fn from(src: Index) -> Self {
        IOIndexResult {
            strings: src
                .tree
                .get_string_iter()
                .map(|(string_id, string)| (string_id.clone(), string.to_string()))
                .collect::<HashMap<usize, String>>(),
            tree: src
                .tree
                .get_tree_iter()
                .map(|(entry_id, entry)| {
                    (
                        entry_id.clone(),
                        IOTreeEntry {
                            parent_id: entry.parent_index,
                            string_id: entry.string_index,
                        },
                    )
                })
                .collect::<HashMap<usize, IOTreeEntry>>(),
            index: src
                .map
                .into_iter()
                .map(|(gram, offsets)| {
                    let gram_str = THE_ENCODING.decode(&gram).0.to_string();
                    let io_offsets = offsets
                        .into_iter()
                        .map(|offset| IOOffset {
                            file_id: offset.file_id,
                            offset: offset.offset,
                        })
                        .collect::<Vec<IOOffset>>();
                    (gram_str, io_offsets)
                })
                .collect::<_>(),
        }
    }
}

impl Into<Index> for IOIndexResult {
    fn into(self) -> Index {
        let mut tree = CompressedTree::new_empty();

        // Reconstruct strings
        for (string_id, string) in self.strings {
            tree.soft_set_string(string_id, string).ok();
        }

        // Reconstruct tree entries
        for (entry_id, io_entry) in self.tree {
            tree.soft_set_tree_entry(
                entry_id,
                TreeEntry {
                    parent_index: io_entry.parent_id,
                    string_index: io_entry.string_id,
                },
            )
            .ok();
        }

        // Reconstruct index map
        let mut map = IndexMap::new();
        for (gram_str, io_offsets) in self.index {
            let gram_bytes = THE_ENCODING.encode(&gram_str).0;
            // Convert to fixed-size array
            if gram_bytes.len() == super::index::GRAM_SIZE {
                let mut gram = [0u8; super::index::GRAM_SIZE];
                gram.copy_from_slice(&gram_bytes[0..super::index::GRAM_SIZE]);

                for io_offset in io_offsets {
                    map.insert(
                        gram,
                        Offset {
                            file_id: io_offset.file_id,
                            offset: io_offset.offset,
                        },
                    );
                }
            }
        }

        Index { tree, map }
    }
}

pub async fn write_index_result(
    index_result: Index,
    out: &mut (impl AsyncWriteExt + std::marker::Unpin),
) -> Result<(), Error> {
    // format: [32 bytes hash][data], 
    // hash added just in case postcard goes rouge
    let io_index_result: IOIndexResult = index_result.into();
    let bytes = postcard::to_stdvec(&io_index_result).map_error(dbg_loc!())?;
    let hash_value = hash(&bytes);
    out.write_all(hash_value.as_bytes())
        .await
        .map_error(dbg_loc!())?;
    out.write_all(&bytes).await.map_error(dbg_loc!())?;
    Ok(())
}

pub async fn read_index_result(mut input: Pin<&mut impl AsyncReadExt>) -> Result<Index, Error> {
    // format: [32 bytes hash][data]
    let mut contents = Vec::new();
    input
        .read_to_end(&mut contents)
        .await
        .map_error(dbg_loc!())?;
    let (hash_bytes, data_bytes) = match contents.split_at_checked(32) {
        Some(x) => x,
        None => {
            let x = Err("Data too short to contain hash"
                .to_string()
                .into_error(dbg_loc!()));
            return x;
        }
    };
    let computed_hash = hash(data_bytes);
    if hash_bytes != computed_hash.as_bytes() {
        return Err("Cache file corrupted.".to_string().into_error(dbg_loc!()));
    }
    let io_index_result: IOIndexResult = postcard::from_bytes(&data_bytes).map_error(dbg_loc!())?;
    Ok(io_index_result.into())
}
