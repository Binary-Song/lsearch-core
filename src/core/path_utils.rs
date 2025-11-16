use crate::prelude::*;
use path_clean::PathClean;
use std::path::Component;

pub trait PathExtension {}

pub trait PathBufExtension {
    fn normalize(self) -> Self;
}

impl PathBufExtension for PathBuf {
    fn normalize(self) -> Self {
        path_clean::PathClean::clean(&self)
    }
}
