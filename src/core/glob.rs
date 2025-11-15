use super::error::Error;
use crate::core::error::{IntoError, MapError};
use crate::core::progress::Progress;
use crate::prelude::*;
use async_recursion::async_recursion;
pub use compressed_tree::CompressedTree;
use globset::{Glob, GlobSet, GlobSetBuilder};
use itertools::Itertools;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct GlobArgs {
    pub target_dir: PathBuf,
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
}

/// A file or directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    /// The parent dir of this entry. An index into `CompressedTree.tree`.
    pub parent_index: usize,
    /// The base name (last path component) of this entry. An index into `CompressedTree.strings`.
    pub string_index: usize,
}

/// This module is to protect the internal fields of CompressedTree from being accessed wildly.
/// Key, index, id are the same thing here (sry for the confusion)
mod compressed_tree {
    use super::*;
    #[derive(Debug, Clone)]
    pub struct CompressedTree {
        _strings: HashMap<usize, String>,
        _tree: HashMap<usize, TreeEntry>,
        _next_string_id: usize,
        _next_tree_id: usize,
    }
    enum DupKeyAction {
        Error,
        CheckEqual,
    }
    /// Adds kv pair to an internal hash map. Returns error if key already exists, the key if successful.
    /// - `key`: Optional key to use. If None, auto-increment `key_gen` to get a new key.
    /// - `val`: The value to insert.
    /// - `map`: The hash map to insert into.
    /// - `id_to_err`: A function that generates an Error given a key and a bool indicating whether
    ///  the key was auto-generated.
    #[inline(always)]
    fn add_item<V: PartialEq>(
        key: Option<usize>,
        key_gen: &mut usize,
        val: V,
        map: &mut HashMap<usize, V>,
        id_to_err: impl Fn(usize, bool) -> Error,
        dup_key_action: DupKeyAction,
    ) -> Result<usize, Error> {
        use std::collections::hash_map::Entry;
        let should_gen_key = key.is_none();
        let key = match key {
            Some(k) => k,
            None => {
                let key = *key_gen;
                *key_gen += 1;
                key
            }
        };
        match map.entry(key) {
            Entry::Occupied(mut _occ_ent) => match dup_key_action {
                DupKeyAction::Error => {
                    return id_to_err(key, should_gen_key).wrap_err();
                }
                DupKeyAction::CheckEqual => {
                    // check equality
                    let existing_val = _occ_ent.get();
                    if *existing_val != val {
                        return id_to_err(key, should_gen_key).wrap_err();
                    }
                    return Ok(key);
                }
            },
            Entry::Vacant(ent) => {
                ent.insert(val); // map[id] = s
                return Ok(key);
            }
        }
    }

    impl CompressedTree {
        pub fn new_empty() -> Self {
            Self {
                _strings: HashMap::new(),
                _tree: HashMap::new(),
                _next_string_id: 0,
                _next_tree_id: 0,
            }
        }
        pub fn new(root_dir: String) -> Self {
            let mut c = CompressedTree {
                _strings: HashMap::new(),
                _tree: HashMap::new(),
                _next_string_id: 1,
                _next_tree_id: 1,
            };
            c._strings.insert(0, root_dir);
            c._tree.insert(
                0,
                TreeEntry {
                    parent_index: 0,
                    string_index: 0,
                },
            );
            c
        }

        pub fn get_tree_at(&self, index: usize) -> Option<TreeEntry> {
            self._tree.get(&index).cloned()
        }

        pub fn get_tree_iter(&self) -> impl Iterator<Item = (&usize, &TreeEntry)> {
            self._tree.iter()
        }

        pub fn get_tree_len(&self) -> usize {
            self._tree.len()
        }

        pub fn get_tree_entry(&self, index: usize) -> Option<TreeEntry> {
            self._tree.get(&index).cloned()
        }

        pub fn get_string(&self, index: usize) -> Option<String> {
            self._strings.get(&index).cloned()
        }

        pub fn add_string(&mut self, s: String) -> Result<usize, Error> {
            add_item(
                None,
                &mut self._next_string_id,
                s,
                &mut self._strings,
                |id, is_auto_key| {
                    format!("String ID collision: {}, auto: {}", id, is_auto_key)
                        .into_error(dbg_loc!())
                },
                DupKeyAction::Error,
            )
        }

        pub fn add_tree_entry(&mut self, tree_ent: TreeEntry) -> Result<usize, Error> {
            add_item(
                None,
                &mut self._next_tree_id,
                tree_ent,
                &mut self._tree,
                |id, is_auto_key| {
                    format!("TreeEntry ID collision: {}, auto: {}", id, is_auto_key)
                        .into_error(dbg_loc!())
                },
                DupKeyAction::Error,
            )
        }

        pub fn get_string_iter(&self) -> impl Iterator<Item = (&usize, &String)> {
            self._strings.iter()
        }

        pub fn merge(&mut self, other: Self) {
            for (id, s) in other._strings {
                self.soft_set_string(id, s).ok();
            }
            for (id, tree_ent) in other._tree {
                self.soft_set_tree_entry(id, tree_ent).ok();
            }
            self._next_string_id = self._next_string_id.max(other._next_string_id);
            self._next_tree_id = self._next_tree_id.max(other._next_tree_id);
        }

        pub fn soft_set_string(&mut self, id: usize, s: String) -> Result<usize, Error> {
            add_item(
                Some(id),
                &mut 0,
                s,
                &mut self._strings,
                |id, is_auto_key| {
                    format!(
                        "Trying to reassign ID a different value. Id: {}, auto: {}",
                        id, is_auto_key
                    )
                    .into_error(dbg_loc!())
                },
                DupKeyAction::CheckEqual,
            )
        }

        pub fn soft_set_tree_entry(
            &mut self,
            id: usize,
            tree_ent: TreeEntry,
        ) -> Result<usize, Error> {
            add_item(
                Some(id),
                &mut 0,
                tree_ent,
                &mut self._tree,
                |id, is_auto_key| {
                    format!(
                        "Trying to reassign ID a different value. Id: {}, auto: {}",
                        id, is_auto_key
                    )
                    .into_error(dbg_loc!())
                },
                DupKeyAction::CheckEqual,
            )
        }
    }
}

impl CompressedTree {
    pub fn get_path(&self, index: usize) -> Result<PathBuf, Error> {
        self.get_path_and_record_dependency(index, None)
    }

    pub fn get_path_and_record_dependency(
        &self,
        index: usize,
        mut dep_tree: Option<&mut Self>,
    ) -> Result<PathBuf, Error> {
        // helper functions to record the used entries in self into dep_tree
        let record_string = |string_id: usize, string: String, dep_tree: &mut Option<&mut Self>| {
            dep_tree
                .as_mut()
                .map(|dep_tree| dep_tree.soft_set_string(string_id, string));
        };
        let record_tree_entry =
            |tree_index: usize, tree_entry: TreeEntry, dep_tree: &mut Option<&mut Self>| {
                dep_tree
                    .as_mut()
                    .map(|dep_tree| dep_tree.soft_set_tree_entry(tree_index, tree_entry));
            };
        // this is the result path. will be reversed later
        let mut components = Vec::new();
        let mut current_tree_id = index;
        // walk up the chain to build the full path
        while current_tree_id != 0 {
            let entry = self
                .get_tree_entry(current_tree_id)
                .ok_or(format!("Invalid tree index: {}", current_tree_id).into_error(dbg_loc!()))?;

            record_tree_entry(current_tree_id, entry.clone(), &mut dep_tree);

            let name = self.get_string(entry.string_index).ok_or(
                format!("Invalid string index: {}", entry.string_index).into_error(dbg_loc!()),
            )?;

            record_string(entry.string_index, name.clone(), &mut dep_tree);

            components.push(name.clone());
            current_tree_id = entry.parent_index;
        }
        let path_at_0 = self
            .get_string(0)
            .ok_or(format!("Invalid string index: 0",).into_error(dbg_loc!()))?;
        components.push(path_at_0.clone());
        record_string(0, path_at_0, &mut dep_tree);

        let mut path = PathBuf::new();
        for component in components.iter().rev() {
            path.push(component);
        }
        return Ok(path);
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
            .map(|pattern_str| Glob::new(pattern_str).map_error(dbg_loc!()))
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
) -> Result<usize, Error> {
    let string_index = compressed_tree.add_string(base_name.to_string())?;
    let tree_entry = TreeEntry {
        parent_index,
        string_index,
    };
    compressed_tree.add_tree_entry(tree_entry)
}

/// Add files and directories from a directory recursively to the compressed tree.
/// Only returns error when yielder is unable to yield progress.
/// Read file errors are reported via yielder.
///
/// - `dir_path`: The path of the directory to glob.
/// - `dir_index`: The `CompressedTree::tree` index of `dir_path` in the compressed tree.
/// - `inc_patts`: The inclusion patterns.
/// - `exc_patts`: The exclusion patterns.
/// - `result`: The compressed tree to add to.
///
/// - `report_progress_interval`: The interval at which to report progress.
///
#[async_recursion]
async fn glob_dir_recursive(
    dir_path: &Path,
    dir_index: usize,
    inc_patts: &GlobSet,
    exc_patts: &GlobSet,
    report_counter: &mut usize,
    yielder: &mut tokio::sync::mpsc::Sender<Progress>,
    result: &mut CompressedTree,
) -> Result<(), Error> {
    let entries = match fs::read_dir(dir_path) {
        Ok(entries) => entries,
        Err(e) => {
            yielder
                .send(Progress::ErrorOccurred {
                    message: format!("{e}"),
                })
                .await
                .map_error(dbg_loc!())?;
            return Ok(());
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                yielder
                    .send(Progress::ErrorOccurred {
                        message: format!("{e}"),
                    })
                    .await
                    .map_error(dbg_loc!())?;
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
            let dir_index = add_to_compressed_tree(dir_index, &base_name, result)?;
            glob_dir_recursive(
                &path,
                dir_index,
                inc_patts,
                exc_patts,
                report_counter,
                yielder,
                result,
            )
            .await?;
        } else if path.is_file() && inc_patts.is_match(&path) && !exc_patts.is_match(&path) {
            add_to_compressed_tree(dir_index, &base_name, result)?;
            *report_counter += 1;
            yielder
                .send(Progress::GlobUpdated {
                    entries: *report_counter,
                })
                .await
                .map_error(dbg_loc!())?;
        }
    }
    return Ok(());
}

pub async fn glob(
    args: &GlobArgs,
    mut yielder: tokio::sync::mpsc::Sender<Progress>,
) -> Result<CompressedTree, Error> {
    let inc_patts_res = compile_patterns(&args.includes);
    let exc_patts_res = compile_patterns(&args.excludes);
    let (inc_patts, exc_patts) = match (inc_patts_res, exc_patts_res) {
        (CompiledPatterns::Err { errs: errs1 }, CompiledPatterns::Err { errs: errs2 }) => {
            let mut errs = errs1;
            errs.extend(errs2);
            return errs.into_error(dbg_loc!()).wrap_err();
        }
        (CompiledPatterns::Err { errs }, _) | (_, CompiledPatterns::Err { errs }) => {
            return errs.into_error(dbg_loc!()).wrap_err();
        }
        (CompiledPatterns::Ok { patts: inc_patts }, CompiledPatterns::Ok { patts: exc_patts }) => {
            (inc_patts, exc_patts)
        }
    };
    let target_dir = match args.target_dir.to_str() {
        Some(s) => s,
        None => {
            return format!("Target dir path is not valid UTF-8: {:?}", args.target_dir)
                .into_error(dbg_loc!())
                .wrap_err();
        }
    };
    let mut compressed_tree = CompressedTree::new(target_dir.to_string());
    glob_dir_recursive(
        args.target_dir.as_ref(),
        0,
        &inc_patts,
        &exc_patts,
        &mut 0,
        &mut yielder,
        &mut compressed_tree,
    )
    .await?;
    return Ok(compressed_tree);
}
