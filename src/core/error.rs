use std::{
    fmt::{Debug, Display},
    io,
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
    BadRequest,
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
                write!(f, "Cannot open file {}", inner_err)?;
            }
            Error::CannotRead { inner_err } => {
                write!(f, "Cannot read file {}", inner_err)?;
            }
            Error::TaskDiedWithJoinError { inner } => {
                write!(f, "Task panicked: {}", inner)?;
            }
            Error::JsonError { inner } => {
                write!(f, "Serde JSON failed: {}", inner)?;
            }
            Error::CannotWrite { inner_err } => write!(f, "Cannot write to file: {}", inner_err)?,
            Error::LogicalError { message } => write!(f, "Logical error: {}", message)?,
            Error::TaskClosedTheChannel => write!(f, "Task closed the channel unexpectedly")?,
            Error::SendError { message } => write!(f, "Send error: {}", message)?,
            Error::RecvError { message } => write!(f, "Receive error: {}", message)?,
            Error::BadRequest => write!(f, "Bad Request",)?,
        }
        Ok(())
    }
}
