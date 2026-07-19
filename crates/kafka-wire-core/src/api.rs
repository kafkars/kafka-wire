//! API key, API version, and bounded version-range vocabulary.
//!
//! These newtypes preserve extensible numeric wire spaces without introducing
//! Kafka operation names into the wire crate.

use std::fmt;

/// Kafka API key as carried on the wire.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiKey(i16);

impl ApiKey {
    /// Creates an API key from its raw wire value.
    pub const fn new(value: i16) -> Self {
        Self(value)
    }

    /// Returns the raw wire value.
    pub const fn value(self) -> i16 {
        self.0
    }
}

impl fmt::Display for ApiKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Kafka message version as carried on the wire.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiVersion(i16);

impl ApiVersion {
    /// Creates an API version from its raw wire value.
    pub const fn new(value: i16) -> Self {
        Self(value)
    }

    /// Returns the raw wire value.
    pub const fn value(self) -> i16 {
        self.0
    }
}

impl fmt::Display for ApiVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Inclusive bounded range of Kafka API versions.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct VersionRange {
    min: ApiVersion,
    max: ApiVersion,
}

impl VersionRange {
    /// Creates an inclusive range.
    ///
    /// Callers should provide `min <= max`; generated constants are validated
    /// before emission.
    pub const fn new(min: i16, max: i16) -> Self {
        Self {
            min: ApiVersion::new(min),
            max: ApiVersion::new(max),
        }
    }

    /// Returns the minimum supported version.
    pub const fn min(self) -> ApiVersion {
        self.min
    }

    /// Returns the maximum supported version.
    pub const fn max(self) -> ApiVersion {
        self.max
    }

    /// Returns whether the range contains `version`.
    pub const fn contains(self, version: ApiVersion) -> bool {
        version.value() >= self.min.value() && version.value() <= self.max.value()
    }
}

impl fmt::Display for VersionRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}-{}", self.min, self.max)
    }
}
