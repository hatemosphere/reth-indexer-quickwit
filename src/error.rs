use std::fmt;

#[derive(Debug)]
#[allow(dead_code)]
pub enum IndexerError {
    Config(String),
    Database(String),
    Provider(String),
    Decode(String),
    Io(std::io::Error),
    Json(serde_json::Error),
    Quickwit(String),
}

impl fmt::Display for IndexerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexerError::Config(msg) => write!(f, "Configuration error: {}", msg),
            IndexerError::Database(msg) => write!(f, "Database error: {}", msg),
            IndexerError::Provider(msg) => write!(f, "Provider error: {}", msg),
            IndexerError::Decode(msg) => write!(f, "Decode error: {}", msg),
            IndexerError::Io(err) => write!(f, "IO error: {}", err),
            IndexerError::Json(err) => write!(f, "JSON error: {}", err),
            IndexerError::Quickwit(msg) => write!(f, "Quickwit error: {}", msg),
        }
    }
}

impl std::error::Error for IndexerError {}

impl From<std::io::Error> for IndexerError {
    fn from(err: std::io::Error) -> Self {
        IndexerError::Io(err)
    }
}

impl From<serde_json::Error> for IndexerError {
    fn from(err: serde_json::Error) -> Self {
        IndexerError::Json(err)
    }
}

#[allow(dead_code)]
pub type Result<T> = std::result::Result<T, IndexerError>;
