use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct HaystackError {
    details: String,
}

impl HaystackError {
    pub fn new(msg: &str) -> HaystackError {
        HaystackError {
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for HaystackError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for HaystackError {
    fn description(&self) -> &str {
        &self.details
    }
}
