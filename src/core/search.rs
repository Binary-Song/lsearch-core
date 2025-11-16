use super::error::Error;
use super::index::Index;
use crate::core::index::Gram;
use crate::core::io::read_index_result;
use crate::core::progress::Progress;
use crate::prelude::*;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tokio::fs::File;

#[derive(Debug, Clone)]
pub struct SearchArgs {
    pub index_files: Vec<PathBuf>,
    pub workers: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchResult {
    pub file_path: PathBuf,
    pub offsets: Vec<usize>,
}

/// Search for a substring in the index
pub async fn search_in_index_file(
    file: &Path,
    query: &[u8],
    _sender: tokio::sync::mpsc::Sender<Progress>,
) -> Result<Vec<SearchResult>, Error> {
    // Read the index file
    let mut index_file = File::open(file).await.map_error(dbg_loc!())?;
    let index_result = read_index_result(Pin::new(&mut index_file)).await?;

    // Perform the search
    search_in_index(index_result, query).await
}

/// Search for a substring in an IndexResult
async fn search_in_index(
    index_result: Index,
    substring: &[u8],
) -> Result<Vec<SearchResult>, Error> {
    let mut results: Vec<SearchResult> = Vec::new();
    let gram_size = 4;

    if substring.len() < gram_size {
        return Ok(results);
    }

    // Generate non-overlapping 4-grams from the substring
    // We need to cover the entire string, so the last gram must end at substring.len()
    let mut grams: Vec<(usize, Gram)> = Vec::new(); // (offset_in_substring, gram)
    let mut i = 0;
    while i + gram_size <= substring.len() {
        let gram: Gram = substring[i..i + gram_size]
            .try_into()
            .map_err(|e: std::array::TryFromSliceError| e.into_error(dbg_loc!()))?;
        grams.push((i, gram));
        
        // If this is not the last possible gram, jump by gram_size for non-overlapping
        // Otherwise, ensure we have a gram that ends at substring.len()
        if i + gram_size < substring.len() && i + gram_size + gram_size > substring.len() {
            // Next jump would overshoot, so position the last gram to end at substring.len()
            i = substring.len() - gram_size;
        } else {
            i += gram_size;
        }
    }

    // Find files that contain all grams
    let mut candidate_files: HashSet<usize> = HashSet::new();
    let mut is_first_gram = true;

    for (_offset, gram) in &grams {
        // Check if this gram exists in the index
        let offsets_for_gram = match index_result.map.get(gram) {
            Some(offsets) => offsets,
            None => {
                // If any gram is not found, no results possible
                return Ok(results);
            }
        };

        // Get the set of files that contain this gram
        let mut files_with_this_gram: HashSet<usize> = HashSet::new();
        for offset in offsets_for_gram {
            files_with_this_gram.insert(offset.file_id);
        }

        if is_first_gram {
            // For the first gram, all files with this gram are candidates
            candidate_files = files_with_this_gram;
            is_first_gram = false;
        } else {
            // For subsequent grams, only keep files that contain all previous grams
            candidate_files.retain(|file_id| files_with_this_gram.contains(file_id));
        }

        // If no candidates remain, no results possible
        if candidate_files.is_empty() {
            return Ok(results);
        }
    }

    // For each candidate file, verify the grams appear at the correct relative positions
    for file_id in candidate_files {
        let mut matching_offsets: Vec<usize> = Vec::new();

        // Get positions of the first gram
        let (first_offset_in_substring, first_gram) = &grams[0];
        let first_gram_positions = index_result
            .map
            .get(first_gram)
            .unwrap()
            .iter()
            .filter(|offset| offset.file_id == file_id)
            .map(|offset| offset.offset)
            .collect::<Vec<usize>>();

        for start_pos in first_gram_positions {
            let mut is_valid_match = true;

            // Check if all subsequent grams appear at the expected positions
            // Each gram should be offset by its position in the substring
            for (offset_in_substring, gram) in grams.iter().skip(1) {
                let expected_pos = start_pos + (offset_in_substring - first_offset_in_substring);
                let gram_positions = index_result
                    .map
                    .get(gram)
                    .unwrap()
                    .iter()
                    .filter(|offset| offset.file_id == file_id)
                    .map(|offset| offset.offset)
                    .collect::<Vec<usize>>();

                if !gram_positions.contains(&expected_pos) {
                    is_valid_match = false;
                    break;
                }
            }

            if is_valid_match {
                matching_offsets.push(start_pos);
            }
        }

        // If we found matches in this file, add to results
        if !matching_offsets.is_empty() {
            let file_path = index_result.tree.get_path(file_id)?;
            matching_offsets.sort();
            results.push(SearchResult {
                file_path: file_path.normalize(),
                offsets: matching_offsets,
            });
        }
    }

    Ok(results)
}

pub async fn search_in_index_files(
    args: SearchArgs,
    query: Vec<u8>,
    sender: tokio::sync::mpsc::Sender<Progress>,
) -> Result<Vec<SearchResult>, Error> {
    use tokio::task::JoinSet;

    let mut join_set = JoinSet::new();
    let total_files = args.index_files.len();

    // Spawn search tasks for each index file
    for index_file in args.index_files {
        let query = query.to_vec();
        let sender = sender.clone();
        join_set.spawn(async move {
            search_in_index_file(&index_file, &query, sender).await
        });
    }

    let mut all_results = Vec::new();
    let mut finished_files = 0;

    // Collect results from all tasks
    while let Some(res) = join_set.join_next().await {
        finished_files += 1;
        match res {
            Ok(Ok(results)) => {
                all_results.extend(results);
                let _ = sender
                    .send(Progress::FileSearched {
                        finished_files,
                        total_files,
                    })
                    .await;
            }
            Ok(Err(e)) => {
                let _ = sender
                    .send(Progress::ErrorOccurred {
                        message: format!("Search error: {}", e),
                    })
                    .await;
            }
            Err(e) => {
                let _ = sender
                    .send(Progress::ErrorOccurred {
                        message: format!("Task error: {}", e),
                    })
                    .await;
            }
        }
    }

    Ok(all_results)
}
