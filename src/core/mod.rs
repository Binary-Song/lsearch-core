//! This module contains the main logic for indexing and searching files.
//! 
//! The Core must not depend on the interface module.
//! 

mod error;
mod glob;
mod index;
mod io;
mod task;
mod task_manager;