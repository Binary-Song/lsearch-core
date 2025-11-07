use std::{io, path::Display};

#[derive(Debug)]
pub struct Error {
    pub message: String,
    pub inner: Option<Box<dyn DisplayableError>>,
}

pub trait DisplayableError: std::fmt::Debug + Send {
    fn display(&self) -> String;
    fn print_to_stderr(&self) {
        eprintln!("{}", self.display());
    }
}

impl DisplayableError for Error {
    fn display(&self) -> String {
        match &self.inner {
            Some(inner) => format!("{} (Caused by: {})", self.message, inner.display()),
            None => self.message.clone(),
        }
    }
}

impl DisplayableError for io::Error {
    fn display(&self) -> String {
        format!("{}", self)
    }
}

#[derive(Debug)]
pub struct ErrorList {
    pub errors: Vec<Error>,
}

impl Error {
    pub fn new_empty(message: String) -> Self {
        Error {
            message,
            inner: None,
        }
    }
    pub fn new(message: String, inner: impl DisplayableError + 'static) -> Self {
        Error {
            message,
            inner: Some(Box::new(inner)),
        }
    }
    pub fn new_list(message: String, errors: Vec<Error>) -> Self {
        Error {
            message,
            inner: Some(Box::new(ErrorList { errors })),
        }
    }
}

impl DisplayableError for ErrorList {
    fn display(&self) -> String {
        if self.errors.len() == 0 {
            return "Empty error list".to_string();
        }

        if self.errors.len() == 1 {
            return self.errors[0].display();
        }

        let c = self
            .errors
            .iter()
            .enumerate()
            .map(|(i, error)| format!("({}) {}", i + 1, error.display()))
            .collect::<Vec<_>>()
            .join(";");
        format!("Multiple errors: [{}]", c)
    }
}
