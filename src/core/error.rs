use std::{
    array::TryFromSliceError,
    fmt::{Debug, Display},
    io,
};

use tokio::task::JoinError;

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
        DebugLocation {
            file: file!(),
            line: line!(),
            column: column!(),
        }
    };
}

#[derive(Debug)]
pub enum Error {
    InvalidPattern {
        pattern: String,
        dbg_loc: DebugLocation,
        inner: globset::Error,
    },
    InvalidPatterns {
        errs: Vec<Error>,
    },
    CannotOpen {
        file_index: usize,
        inner_err: io::Error,
    },
    CannotRead {
        inner_err: io::Error,
    },
    CannotWrite {
        inner_err: io::Error,
    },
    TaskDiedWithJoinError {
        inner: JoinError,
    },
    TaskClosedTheChannel,
    JsonError {
        reason: &'static str,
        inner: serde_json::Error,
    },
    PostcardError  ,
    SendError {
        message: String,
    },
    RecvError {
        message: String,
    },
    LogicalError {
        message: String,
        dbg_loc: DebugLocation,
    },
    BadRequest,
    DecodeUtf8Error {
        error: std::string::FromUtf8Error,
    },
    CannotConvertSlideToArray {
        error: TryFromSliceError,
    },
}

pub trait IntoError<E> {
    fn into_error(self, dbg_loc: DebugLocation) -> E;
}

// currying
impl IntoError<Box<dyn FnOnce(String) -> Error>> for globset::Error {
    fn into_error(self, dbg_loc: DebugLocation) -> Box<dyn FnOnce(String) -> Error> {
        Box::new(|pattern| Error::InvalidPattern {
            pattern: pattern,
            inner: self,
            dbg_loc: dbg_loc,
        })
    }
}

impl IntoError<Error> for String {
    fn into_error(self, dbg_loc: DebugLocation) -> Error {
        Error::LogicalError {
            message: self,
            dbg_loc: dbg_loc,
        }
    }
}

impl IntoError<Error> for io::Error {
    fn into_error(self, _dbg_loc: DebugLocation) -> Error {
        Error::CannotRead { inner_err: self }
    }
}

impl IntoError<Error> for TryFromSliceError {
    fn into_error(self, _dbg_loc: DebugLocation) -> Error {
        Error::CannotConvertSlideToArray { error: self }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidPattern {
                pattern,
                dbg_loc,
                inner,
            } => {
                write!(f, "Invalid pattern: {}: {}", pattern, inner)?;
            }
            Error::InvalidPatterns { errs } => {
                write!(f, "Invalid patterns:")?;
                for err in errs {
                    write!(f, "\n- {}", err)?;
                }
            }
            Error::CannotOpen {
                file_index,
                inner_err,
            } => {
                write!(f, "Cannot open file {}", inner_err)?;
            }
            Error::CannotRead { inner_err } => {
                write!(f, "Cannot read file {}", inner_err)?;
            }
            Error::TaskDiedWithJoinError { inner } => {
                write!(f, "Task panicked: {}", inner)?;
            }
            Error::JsonError { inner, reason } => {
                write!(f, "Serde JSON failed: {}, {}", inner, reason)?;
            }
            Error::CannotWrite { inner_err } => write!(f, "Cannot write to file: {}", inner_err)?,
            Error::LogicalError { message, dbg_loc } => {
                write!(f, "Logical error: {} ({})", message, dbg_loc)?
            }
            Error::TaskClosedTheChannel => write!(f, "Task closed the channel unexpectedly")?,
            Error::SendError { message } => write!(f, "Send error: {}", message)?,
            Error::RecvError { message } => write!(f, "Receive error: {}", message)?,
            Error::BadRequest => write!(f, "Bad Request",)?,
            Error::DecodeUtf8Error { error } => write!(f, "UTF-8 decode error: {}", error)?,
            Error::CannotConvertSlideToArray { error } => {
                write!(f, "Cannot convert slice to array: {}", error)?
            },
            Error::PostcardError {} => {
                write!(f, "Postcard serialization/deserialization error")?
            }
        }
        Ok(())
    }
}

impl Error {
    pub fn into_result<T>(self) -> Result<T, Error> {
        Err(self)
    }
}
