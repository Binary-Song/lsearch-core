use super::error::Error;
use super::index::IndexResult;
use crate::core::io::read_index_result;
use crate::core::progress::Progress;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tokio::fs::File;

#[derive(Debug, Clone)]
pub struct SearchArgs {
    pub index_files: Vec<String>,
}

#[derive(Debug, Clone)]
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
    let mut index_file = File::open(file).await.map_err(|e| Error::CannotOpen {
        file_index: 0,
        inner_err: e,
    })?;
    
    let index_result = read_index_result(Pin::new(&mut index_file)).await?;
    
    // Perform the search
    search_in_index(index_result, query).await
}

/// Search for a substring in an IndexResult
async fn search_in_index(
    index_result: IndexResult,
    substring: &[u8],
) -> Result<Vec<SearchResult>, Error> {
    let mut results: Vec<SearchResult> = Vec::new();
    let gram_size = 4;

    if substring.len() < gram_size {
        return Ok(results);
    }

    // Generate all 4-grams from the substring
    let mut grams: Vec<Vec<u8>> = Vec::new();
    for i in 0..=(substring.len() - gram_size) {
        grams.push(substring[i..i + gram_size].to_vec());
    }

    // Find files that contain all grams
    let mut candidate_files: HashSet<usize> = HashSet::new();
    let mut is_first_gram = true;

    for gram in &grams {
        // Check if this gram exists in the index
        let offsets_for_gram = match index_result.gram_to_offsets.map.get(gram) {
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

    // For each candidate file, check if the grams appear consecutively
    for file_id in candidate_files {
        let mut matching_offsets: Vec<usize> = Vec::new();

        // Get positions of the first gram
        let first_gram_positions = index_result
            .gram_to_offsets
            .map
            .get(&grams[0])
            .unwrap()
            .iter()
            .filter(|offset| offset.file_id == file_id)
            .map(|offset| offset.offset)
            .collect::<Vec<usize>>();

        for start_pos in first_gram_positions {
            let mut is_valid_match = true;

            // Check if all subsequent grams appear at the expected positions
            for (gram_index, gram) in grams.iter().enumerate().skip(1) {
                let expected_pos = start_pos + gram_index;
                let gram_positions = index_result
                    .gram_to_offsets
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
            let file_path = index_result.compressed_tree.get_path(file_id);
            matching_offsets.sort();
            results.push(SearchResult {
                file_path,
                offsets: matching_offsets,
            });
        }
    }

    Ok(results)
}

pub async fn search_in_index_files(
    _args: SearchArgs,
    _sender: tokio::sync::mpsc::Sender<Progress>,
) -> Result<(), Error> {
    // TODO: Implement searching across multiple index files
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_search_hello() {
        let index_path = PathBuf::from("test_resources/testgrounds.index");
        let (sender, _receiver) = tokio::sync::mpsc::channel(10);

        let query = b"Hello";
        let results = search_in_index_file(&index_path, query, sender)
            .await
            .expect("Search should succeed");

        // We expect to find "Hello" in helloworld.txt
        assert!(!results.is_empty(), "Should find at least one result for 'Hello'");

        // Print results for debugging
        for result in &results {
            println!("Found '{}' in: {:?}", String::from_utf8_lossy(query), result.file_path);
            println!("  {} occurrences at offsets: {:?}", result.offsets.len(), result.offsets);
        }

        // Check that at least one result contains "helloworld.txt" in the path
        let found_in_helloworld = results.iter().any(|r| {
            r.file_path
                .to_string_lossy()
                .contains("helloworld.txt")
        });
        assert!(
            found_in_helloworld,
            "Should find 'Hello' in helloworld.txt"
        );
        
        // Verify we got the expected number of results (20 "Hello" occurrences as shown in the gram debug)
        assert_eq!(results.len(), 1, "Should have exactly 1 file with 'Hello'");
        assert_eq!(results[0].offsets.len(), 20, "Should have 20 occurrences of 'Hello' in the file");
    }
    
    #[tokio::test]
    async fn test_search_shell() {
        let index_path = PathBuf::from("test_resources/testgrounds.index");
        let (sender, _receiver) = tokio::sync::mpsc::channel(10);

        let query = b"Shell";
        let results = search_in_index_file(&index_path, query, sender)
            .await
            .expect("Search should succeed");

        // Print results for debugging
        for result in &results {
            println!("Found '{}' in: {:?}", String::from_utf8_lossy(query), result.file_path);
            println!("  {} occurrences at offsets: {:?}", result.offsets.len(), result.offsets);
        }

        // We should find "Shell" (from PowerShell, Shell, etc.)
        assert!(!results.is_empty(), "Should find at least one result for 'Shell'");
    }
}
