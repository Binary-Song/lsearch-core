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
