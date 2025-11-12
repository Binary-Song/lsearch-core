use super::error::Error;
use super::glob::{glob, CompressedTree, GlobArgs};
use crate::core::io::write_index_result;
use crate::core::progress::Progress;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio::{fs::File, io::AsyncReadExt};
use crate::core::io::read_index_result;

#[derive(Debug, Clone)]
pub struct SearchArgs {
    pub index_files: Vec<String>,
}

pub async fn search_in_index_file(
    file: &Path,
    query: &str,
    sender: tokio::sync::mpsc::Sender<Progress>,
) -> Result<(), Error> {
    
}

pub async fn search_in_index_files(
    args: SearchArgs,
    sender: tokio::sync::mpsc::Sender<Progress>,
) -> Result<(), Error> {

}
