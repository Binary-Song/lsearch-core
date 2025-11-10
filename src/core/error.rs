use std::{
    fmt::{Debug, Display},
    io,
    path::PathBuf,
};

use tokio::task::JoinError;

#[derive(Debug)]
pub enum Error {
    InvalidPattern {
        pattern: String,
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
        file_index: usize,
        inner_err: io::Error,
    },
    CannotWrite {
        inner_err: io::Error,
    },
    TaskDiedWithJoinError {
        inner: JoinError,
    },
    TaskClosedTheChannel,
    SerdeJsonFailed {
        inner: serde_json::Error,
    },
    SendError {
        message: String,
    },
    RecvError {
        message: String,
    },
    LogicalError {
        message: String,
    },
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidPattern { pattern, inner } => {
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
                write!(f, "Cannot open file {}: {}", file_index, inner_err)?;
            }
            Error::CannotRead {
                file_index,
                inner_err,
            } => {
                write!(f, "Cannot read file {}: {}", file_index, inner_err)?;
            }
            Error::TaskDiedWithJoinError { inner } => {
                write!(f, "Task panicked: {}", inner)?;
            }
            Error::SerdeJsonFailed { inner } => {
                write!(f, "Serde JSON failed: {}", inner)?;
            }
            Error::CannotWrite { inner_err } => write!(f, "Cannot write to file: {}", inner_err)?,
            Error::LogicalError { message } => write!(f, "Logical error: {}", message)?,
            Error::TaskClosedTheChannel => write!(f, "Task closed the channel unexpectedly")?,
            Error::SendError { message } => write!(f, "Yield error: {}", message)?,
        }
        Ok(())
    }
}
