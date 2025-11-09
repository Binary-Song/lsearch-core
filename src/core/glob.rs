use super::error::Error;
use globset::{Glob, GlobSet, GlobSetBuilder};
use itertools::Itertools;
use std::ffi::os_str;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::{fs, io};
use tempfile::TempDir;

#[derive(Clone)]
pub struct GlobArgs {
    pub target_dir: String,
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
    pub report_glob_progress_interval: usize,
    pub report_glob_progress_channel: tokio::sync::mpsc::Sender<GlobProgress>,
}

pub enum GlobProgress {
    GlobUpdated { total_entries: usize },
}
/// A file or directory.
#[derive(Debug)]
pub struct TreeEntry {
    /// The parent dir of this entry. An index into `CompressedTree.tree`.
    pub parent_index: usize,
    /// The base name (last path component) of this entry. An index into `CompressedTree.strings`.
    pub string_index: usize,
}

#[derive(Debug)]
pub struct CompressedTree {
    pub strings: Vec<String>,
    pub tree: Vec<TreeEntry>,
}

impl CompressedTree {
    fn new(root_dir: String) -> Self {
        CompressedTree {
            strings: vec![root_dir],
            tree: vec![TreeEntry {
                parent_index: 0,
                string_index: 0,
            }],
        }
    }

    fn add_string(&mut self, s: String) -> usize {
        let index = self.strings.len();
        self.strings.push(s);
        return index;
    }

    pub fn get_path(&self, index: usize) -> PathBuf {
        let mut components = Vec::new();
        let mut current_index = index;
        while current_index != 0 {
            let entry = &self.tree[current_index];
            let name = &self.strings[entry.string_index];
            components.push(name.clone());
            current_index = entry.parent_index;
        }
        components.reverse();
        let path = components.iter().collect::<PathBuf>();
        return path;
    }
}

enum CompiledPatterns {
    Ok { patts: globset::GlobSet },
    Err { errs: Vec<Error> },
}

fn compile_patterns(strs: &Vec<String>) -> CompiledPatterns {
    let strs_to_patts = |patts: &Vec<String>| {
        patts
            .iter()
            .map(|pattern_str| {
                Glob::new(pattern_str).map_err(|e| Error::InvalidPattern {
                    pattern: pattern_str.clone(),
                    inner: e,
                })
            })
            .partition_map(|res| match res {
                Ok(pat) => itertools::Either::Left(pat),
                Err(err) => itertools::Either::Right(err),
            })
    };
    let (patts, errs): (Vec<_>, Vec<_>) = strs_to_patts(&strs);
    if errs.len() != 0 {
        return CompiledPatterns::Err { errs };
    }
    let mut builder = GlobSetBuilder::new();
    for patt in patts.into_iter() {
        builder.add(patt);
    }
    return CompiledPatterns::Ok {
        patts: builder.build().unwrap(),
    };
}

/// Add a path to the compressed tree.
/// - `parent_index`: The `CompressedTree::tree` index of the parent dir of this path.
/// - `base_name`: The base name (last path component) of this path.
/// - `compressed_tree`: The compressed tree to add to.
/// - return: The `CompressedTree::tree` index of the newly added path.
fn add_to_compressed_tree(
    parent_index: usize,
    base_name: &str,
    compressed_tree: &mut CompressedTree,
) -> usize {
    let string_index = compressed_tree.add_string(base_name.to_string());
    compressed_tree.tree.push(TreeEntry {
        parent_index,
        string_index,
    });
    return compressed_tree.tree.len() - 1;
}

/// Add files and directories from a directory recursively to the compressed tree.
/// - `dir_path`: The path of the directory to glob.
/// - `dir_index`: The `CompressedTree::tree` index of `dir_path` in the compressed tree.
/// - `inc_patts`: The inclusion patterns.
/// - `exc_patts`: The exclusion patterns.
/// - `result`: The compressed tree to add to.
///
/// - `report_progress_interval`: The interval at which to report progress.
/// - `current_count`: The current count of processed entries.
/// - `progress_report_channel`: The channel to report progress on.
async fn glob_dir_recursive(
    dir_path: &Path,
    dir_index: usize,
    inc_patts: &GlobSet,
    exc_patts: &GlobSet,
    report_progress_interval: usize,
    report_counter: &mut usize,
    report_progress_channel: &mut tokio::sync::mpsc::Sender<GlobProgress>,
    result: &mut CompressedTree,
) {
    let entries = match fs::read_dir(dir_path) {
        Ok(entries) => entries,
        Err(e) => {
            return;
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                continue;
            }
        };
        let path = entry.path();
        let base_name = match path.iter().last() {
            Some(os_str) => os_str,
            None => continue, // a 0-length component like 'a/b//c' ??
        };
        let base_name = base_name.to_string_lossy();
        let base_name = base_name.as_ref();
        if path.is_symlink() {
            continue; // skip symlinks to avoid cycles
        }
        // the recursive case: glob sub dirs
        if path.is_dir() {
            let dir_index = add_to_compressed_tree(dir_index, &base_name, result);
            glob_dir_recursive(
                &path,
                dir_index,
                inc_patts,
                exc_patts,
                report_progress_interval,
                report_counter,
                report_progress_channel,
                result,
            );
        } else if path.is_file() && inc_patts.is_match(&path) && !exc_patts.is_match(&path) {
            add_to_compressed_tree(dir_index, &base_name, result);
            *report_counter += 1;
            if *report_counter % report_progress_interval == 0 {
                report_progress_channel
                    .send(GlobProgress::GlobUpdated {
                        total_entries: *report_counter,
                    })
                    .await;
            }
        }
    }
}

pub async fn glob(args:  GlobArgs) -> Result<CompressedTree, Error> {
    let inc_patts_res = compile_patterns(&args.includes);
    let exc_patts_res = compile_patterns(&args.excludes);
    let (inc_patts, exc_patts) = match (inc_patts_res, exc_patts_res) {
        (CompiledPatterns::Err { errs: errs1 }, CompiledPatterns::Err { errs: errs2 }) => {
            let mut errs = errs1;
            errs.extend(errs2);
            return Err(Error::InvalidPatterns { errs });
        }
        (CompiledPatterns::Err { errs }, _) | (_, CompiledPatterns::Err { errs }) => {
            return Err(Error::InvalidPatterns { errs });
        }
        (CompiledPatterns::Ok { patts: inc_patts }, CompiledPatterns::Ok { patts: exc_patts }) => {
            (inc_patts, exc_patts)
        }
    };
    let mut compressed_tree = CompressedTree::new(args.target_dir.clone());
    glob_dir_recursive(
        args.target_dir.as_ref(),
        0,
        &inc_patts,
        &exc_patts,
        args.report_glob_progress_interval,
        &mut 0,
        &mut args.report_glob_progress_channel.clone(),
        &mut compressed_tree,
    )
    .await;
    return Ok(compressed_tree);
}

#[cfg(test)]
mod tests {}
