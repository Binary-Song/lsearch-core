use std::{
    array::TryFromSliceError,
    fmt::{Debug, Display},
    io,
};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::{sync::mpsc::error::SendError, task::JoinError};

#[derive(Debug, Clone)]
pub struct DebugLocation {
    pub file: &'static str,
    pub line: u32,
    pub column: u32,
}
impl Display for DebugLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}", self.file, self.line, self.column)
    }
}
#[macro_export]
macro_rules! dbg_loc {
    () => {
        crate::core::error::DebugLocation {
            file: file!(),
            line: line!(),
            column: column!(),
        }
    };
}

/// Standard way of representing errors in the whole project
pub struct Error {
    pub inner: Box<dyn Debug + Send + 'static>,
    pub dbg_loc: DebugLocation,
}

impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error(location: {}, {:?})", self.dbg_loc, self.inner)
    }
}

impl Error {
    pub fn wrap_err<T>(self) -> Result<T, Error> {
        return Err(self);
    }
}

pub trait IntoError {
    fn into_error(self, dbg_loc: DebugLocation) -> Error;
}

impl<T: Debug + Send + 'static> IntoError for T {
    fn into_error(self, dbg_loc: DebugLocation) -> Error {
        Error {
            inner: Box::new(self),
            dbg_loc,
        }
    }
}

pub trait MapError<T> {
    fn map_error(self, dbg_loc: DebugLocation) -> Result<T, Error>;
}

impl<T, E: Debug + Send + 'static> MapError<T> for Result<T, E> {
    fn map_error(self, dbg_loc: DebugLocation) -> Result<T, Error> {
        self.map_err(|e| e.into_error(dbg_loc))
    }
}
