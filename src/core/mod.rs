//! This module contains the main logic for indexing and searching files.
//!
//! The Core must not depend on the interface module.
//!

pub mod error;
pub mod glob;
pub mod index;
pub mod io;
pub mod progress;
pub mod search;

pub use error::Error;
pub use index::index_directory;
pub use index::IndexArgs;
pub use progress::Progress;
