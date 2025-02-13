use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

/// Auto offset reset behavior
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AutoOffsetReset {
    /// Start from the smallest/earliest offset
    Earliest,
    /// Start from the largest/latest offset
    Latest,
    /// Error when no offset is found
    Error,
}

impl Display for AutoOffsetReset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AutoOffsetReset::Earliest => write!(f, "earliest"),
            AutoOffsetReset::Latest => write!(f, "latest"),
            AutoOffsetReset::Error => write!(f, "error"),
        }
    }
}

impl Default for AutoOffsetReset {
    fn default() -> Self {
        AutoOffsetReset::Latest
    }
}
