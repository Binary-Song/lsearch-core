use crate::{core::search::SearchResult, prelude::PathBufExtension, test::EndToEndTest};
use std::path::PathBuf;

#[tokio::test]
async fn precision1() {
    let test = EndToEndTest {
        target_dir: "./test_data/precision_test".to_string(),
        query: "Lorem".to_string(),
        truth: vec![SearchResult {
            file_path: PathBuf::from("./test_data/precision_test/lorem.txt").normalize(),
            offsets: vec![0, 370, 692, 824, 1086, 1265, 1287],
        }],
    };
    test.execute().await;
}

#[tokio::test]
async fn precision2_longer_word() {
    // Test with "ipsum" - a longer word
    let test = EndToEndTest {
        target_dir: "./test_data/precision_test".to_string(),
        query: "ipsum".to_string(),
        truth: vec![SearchResult {
            file_path: PathBuf::from("./test_data/precision_test/lorem.txt").normalize(),
            offsets: vec![6, 376, 650, 698, 830, 1092, 1271, 1293],
        }],
    };
    test.execute().await;
}

#[tokio::test]
async fn precision3_multiple_files() {
    // Test searching for "quick" across multiple files
    let test = EndToEndTest {
        target_dir: "./test_data/precision_test".to_string(),
        query: "quick".to_string(),
        truth: vec![
            SearchResult {
                file_path: PathBuf::from("./test_data/precision_test/file1.txt").normalize(),
                offsets: vec![4, 140, 194],
            },
            SearchResult {
                file_path: PathBuf::from("./test_data/precision_test/file2.txt").normalize(),
                offsets: vec![22, 191],
            },
        ],
    };
    test.execute().await;
}

#[tokio::test]
async fn precision4_programming_language() {
    // Test searching for "Rust" in file3.txt
    let test = EndToEndTest {
        target_dir: "./test_data/precision_test".to_string(),
        query: "Rust".to_string(),
        truth: vec![SearchResult {
            file_path: PathBuf::from("./test_data/precision_test/file3.txt").normalize(),
            offsets: vec![23, 77],
        }],
    };
    test.execute().await;
}

#[tokio::test]
async fn precision5_long_word() {
    // Test searching for "JavaScript" - a longer word
    let test = EndToEndTest {
        target_dir: "./test_data/precision_test".to_string(),
        query: "JavaScript".to_string(),
        truth: vec![SearchResult {
            file_path: PathBuf::from("./test_data/precision_test/file3.txt").normalize(),
            offsets: vec![37, 193],
        }],
    };
    test.execute().await;
}
