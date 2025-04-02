use std::ops::RangeInclusive;
use std::fmt;

/// The Content Range HTTP header with unit "bytes".
pub enum ContentRange {
    Satisfied { range: RangeInclusive<u64>, complete_length: Option<u64> },
    Unsatisfied { complete_length: u64 },
}

impl fmt::Display for ContentRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "bytes ")?;
        match self {
            ContentRange::Satisfied { range, complete_length } => {
                write!(f, "{}-{}/", range.start(), range.end())?;
                match complete_length {
                    None => write!(f, "*"),
                    Some(complete_length) => write!(f, "{complete_length}"),
                }
            },
            ContentRange::Unsatisfied { complete_length } => {
                write!(f, "*/{complete_length}")
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ContentRange;

    #[test]
    fn a0() {
        assert_eq!(
            ContentRange::Unsatisfied { complete_length: 8391 }.to_string(),
            "bytes */8391",
        );
    }

    #[test]
    fn a1() {
        assert_eq!(
            ContentRange::Satisfied {
                range: 0 ..= 9,
                complete_length: Some(20),
            }.to_string(),
            "bytes 0-9/20",
        );
    }

    #[test]
    fn a2() {
        assert_eq!(
            ContentRange::Satisfied {
                range: 0 ..= 9,
                complete_length: None,
            }.to_string(),
            "bytes 0-9/*",
        );
    }
}
