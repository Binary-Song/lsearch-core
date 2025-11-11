//! This module contains the main logic for indexing and searching files.
//!
//! The Core must not depend on the interface module.
//!

mod error;
mod glob;
mod index;
mod io;
mod progress;

pub use error::Error;
pub use index::index_directory;
pub use index::IndexArgs;
pub use progress::Progress;
