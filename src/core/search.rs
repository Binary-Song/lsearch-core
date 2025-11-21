use super::error::Error;
use super::index::{Index, StratificationMetadata};
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
    pub index_dir: PathBuf, // Changed from index_files to index_dir
    pub workers: usize,
}

/// Find relevant group directories for a query based on its grams
async fn find_relevant_groups(
    index_dir: &Path,
    query: &[u8],
) -> Result<Vec<usize>, Error> {
    // Try to load stratification metadata
    let stratification_path = index_dir.join("stratification.json");
    
    let stratification = if let Ok(content) = tokio::fs::read_to_string(&stratification_path).await {
        serde_json::from_str::<StratificationMetadata>(&content).ok()
    } else {
        None
    };
    
    let gram_size = 4;
    if query.len() < gram_size {
        // If no stratification or query too short, return all groups
        return find_all_groups(index_dir).await;
    }
    
    match stratification {
        Some(strat) => {
            // Extract grams from query
            let mut relevant_groups = HashSet::new();
            let mut i = 0;
            while i + gram_size <= query.len() {
                let gram: Gram = query[i..i + gram_size].try_into().map_error(dbg_loc!())?;
                
                // Find which group this gram belongs to
                for (group_id, range) in strat.groups.iter().enumerate() {
                    if range.contains(&gram) {
                        relevant_groups.insert(group_id);
                        break;
                    }
                }
                
                // Move to next gram (non-overlapping or adjusted for coverage)
                if i + gram_size < query.len() && i + gram_size + gram_size > query.len() {
                    i = query.len() - gram_size;
                } else {
                    i += gram_size;
                }
            }
            
            Ok(relevant_groups.into_iter().collect())
        }
        None => {
            // No stratification, search all groups
            find_all_groups(index_dir).await
        }
    }
}

/// Find all group directories in the index directory
async fn find_all_groups(index_dir: &Path) -> Result<Vec<usize>, Error> {
    let mut groups = Vec::new();
    
    let mut entries = tokio::fs::read_dir(index_dir).await.map_error(dbg_loc!())?;
    while let Some(entry) = entries.next_entry().await.map_error(dbg_loc!())? {
        let path = entry.path();
        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(group_id_str) = name.strip_prefix("group_") {
                    if let Ok(group_id) = group_id_str.parse::<usize>() {
                        groups.push(group_id);
                    }
                }
            }
        }
    }
    
    // If no groups found, assume old structure (single directory)
    if groups.is_empty() {
        groups.push(0);
    }
    
    groups.sort();
    Ok(groups)
}

/// Get all index files in specified groups
async fn get_index_files_for_groups(
    index_dir: &Path,
    groups: &[usize],
) -> Result<Vec<PathBuf>, Error> {
    let mut index_files = Vec::new();
    
    for &group_id in groups {
        let group_dir = if group_id == 0 && !index_dir.join("group_0").exists() {
            // Old structure - files directly in index_dir
            index_dir.to_path_buf()
        } else {
            index_dir.join(format!("group_{}", group_id))
        };
        
        if !group_dir.exists() {
            continue;
        }
        
        let mut entries = tokio::fs::read_dir(&group_dir).await.map_error(dbg_loc!())?;
        while let Some(entry) = entries.next_entry().await.map_error(dbg_loc!())? {
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("bin") {
                index_files.push(path);
            }
        }
    }
    
    Ok(index_files)
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

    // Find relevant groups for this query
    let relevant_groups = find_relevant_groups(&args.index_dir, &query).await?;
    debug!("Searching in {} groups: {:?}", relevant_groups.len(), relevant_groups);
    
    // Get index files for these groups
    let index_files = get_index_files_for_groups(&args.index_dir, &relevant_groups).await?;
    debug!("Found {} index files to search", index_files.len());
    
    let mut join_set = JoinSet::new();
    let total_files = index_files.len();

    // Spawn search tasks for each index file
    for index_file in index_files {
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
