//! Parsed and normalized Kafka version-set algebra.

use std::{fmt, str::FromStr};

use thiserror::Error;

/// One inclusive version interval, optionally open-ended.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct VersionRange {
    start: i16,
    end: Option<i16>,
}

impl VersionRange {
    /// Creates a bounded inclusive range.
    pub const fn bounded(start: i16, end: i16) -> Self {
        Self {
            start,
            end: Some(end),
        }
    }

    /// Creates an open-ended inclusive range.
    pub const fn open(start: i16) -> Self {
        Self { start, end: None }
    }

    /// Returns the inclusive first version.
    pub const fn start(self) -> i16 {
        self.start
    }

    /// Returns the inclusive last version, or `None` for an open range.
    pub const fn end(self) -> Option<i16> {
        self.end
    }

    /// Returns whether this interval contains `version`.
    pub const fn contains(self, version: i16) -> bool {
        version >= self.start
            && match self.end {
                Some(end) => version <= end,
                None => true,
            }
    }
}

/// A normalized union of disjoint inclusive version ranges.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct VersionSet {
    ranges: Vec<VersionRange>,
}

impl VersionSet {
    /// Returns an empty set.
    pub const fn none() -> Self {
        Self { ranges: Vec::new() }
    }

    /// Returns the normalized ranges.
    pub fn ranges(&self) -> &[VersionRange] {
        &self.ranges
    }

    /// Returns whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Returns whether the set contains `version`.
    pub fn contains(&self, version: i16) -> bool {
        self.ranges.iter().any(|range| range.contains(version))
    }

    /// Returns the intersection with another set.
    #[must_use]
    pub fn intersection(&self, other: &Self) -> Self {
        let mut ranges = Vec::new();
        for left in &self.ranges {
            for right in &other.ranges {
                if let Some(range) = intersect(*left, *right) {
                    ranges.push(range);
                }
            }
        }
        Self::normalized(ranges)
    }

    /// Returns the versions in this set that `other` does not contain.
    ///
    /// Needed because a field's nullability can cut its presence window in two:
    /// `MetadataRequest.Topics` appears from v0 and is nullable from v1, and the
    /// codec for the versions where it is *not* nullable has to be chosen over
    /// exactly the remainder.
    ///
    /// Computed by walking the versions rather than by interval arithmetic. An
    /// open-ended range has no last version to subtract from, so the walk is
    /// bounded by whichever endpoint the two sets actually name; a difference
    /// against an open range beyond that point is empty by construction.
    #[must_use]
    pub fn difference(&self, other: &Self) -> Self {
        let Some(highest) = self.highest() else {
            return Self::none();
        };
        let mut ranges: Vec<VersionRange> = Vec::new();
        for version in self.lowest().unwrap_or(0)..=highest {
            if !self.contains(version) || other.contains(version) {
                continue;
            }
            match ranges.last_mut() {
                Some(last) if last.end == Some(version - 1) => last.end = Some(version),
                _ => ranges.push(VersionRange::bounded(version, version)),
            }
        }
        Self::normalized(ranges)
    }

    /// The first version this set names, if any.
    fn lowest(&self) -> Option<i16> {
        self.ranges.first().map(|range| range.start)
    }

    /// The last version this set names, treating an open range as unbounded.
    ///
    /// `None` for an empty set and for one that runs to infinity, where a
    /// difference cannot be enumerated and no caller here produces one: every
    /// window this is asked about has already been intersected with a message's
    /// bounded `validVersions`.
    fn highest(&self) -> Option<i16> {
        self.ranges.last().and_then(|range| range.end)
    }

    /// Returns whether every represented version is contained by `other`.
    pub fn is_subset_of(&self, other: &Self) -> bool {
        self.ranges
            .iter()
            .all(|range| range_is_covered(*range, other))
    }

    /// Returns a single bounded interval when the set has exactly that shape.
    pub fn single_bounded(&self) -> Option<(i16, i16)> {
        match self.ranges.as_slice() {
            [range] => range.end.map(|end| (range.start, end)),
            _ => None,
        }
    }

    fn normalized(mut ranges: Vec<VersionRange>) -> Self {
        ranges.sort_by_key(|range| range.start);
        let mut normalized: Vec<VersionRange> = Vec::new();
        for current in ranges {
            let Some(previous) = normalized.last_mut() else {
                normalized.push(current);
                continue;
            };

            let touches = match previous.end {
                None => true,
                Some(end) => current.start <= end.saturating_add(1),
            };
            if touches {
                previous.end = max_end(previous.end, current.end);
            } else {
                normalized.push(current);
            }
        }
        Self { ranges: normalized }
    }
}

impl FromStr for VersionSet {
    type Err = VersionParseError;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        let source = source.trim();
        if source.eq_ignore_ascii_case("none") || source.is_empty() {
            return Ok(Self::none());
        }

        let mut ranges = Vec::new();
        for term in source.split(',').map(str::trim) {
            if let Some(start) = term.strip_suffix('+') {
                ranges.push(VersionRange::open(parse_version(start, term)?));
                continue;
            }
            if let Some((start, end)) = term.split_once('-') {
                let start = parse_version(start, term)?;
                let end = parse_version(end, term)?;
                if end < start {
                    return Err(VersionParseError::Descending {
                        term: term.to_owned(),
                    });
                }
                ranges.push(VersionRange::bounded(start, end));
                continue;
            }
            let version = parse_version(term, term)?;
            ranges.push(VersionRange::bounded(version, version));
        }

        Ok(Self::normalized(ranges))
    }
}

impl fmt::Display for VersionSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.ranges.is_empty() {
            return formatter.write_str("none");
        }
        for (index, range) in self.ranges.iter().enumerate() {
            if index > 0 {
                formatter.write_str(",")?;
            }
            match range.end {
                None => write!(formatter, "{}+", range.start)?,
                Some(end) if end == range.start => write!(formatter, "{}", range.start)?,
                Some(end) => write!(formatter, "{}-{end}", range.start)?,
            }
        }
        Ok(())
    }
}

/// Invalid upstream version expression.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum VersionParseError {
    /// A term was not a non-negative `i16`.
    #[error("invalid version term `{term}`")]
    Invalid {
        /// Invalid term.
        term: String,
    },
    /// An inclusive range ended before it began.
    #[error("descending version range `{term}`")]
    Descending {
        /// Invalid range.
        term: String,
    },
}

fn parse_version(value: &str, term: &str) -> Result<i16, VersionParseError> {
    value
        .trim()
        .parse::<i16>()
        .ok()
        .filter(|version| *version >= 0)
        .ok_or_else(|| VersionParseError::Invalid {
            term: term.to_owned(),
        })
}

fn intersect(left: VersionRange, right: VersionRange) -> Option<VersionRange> {
    let start = left.start.max(right.start);
    let end = min_end(left.end, right.end);
    if end.is_some_and(|end| end < start) {
        None
    } else {
        Some(VersionRange { start, end })
    }
}

fn min_end(left: Option<i16>, right: Option<i16>) -> Option<i16> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(end), None) | (None, Some(end)) => Some(end),
        (None, None) => None,
    }
}

fn max_end(left: Option<i16>, right: Option<i16>) -> Option<i16> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (None, _) | (_, None) => None,
    }
}

fn range_is_covered(range: VersionRange, set: &VersionSet) -> bool {
    set.ranges.iter().any(|candidate| {
        if candidate.start > range.start {
            return false;
        }
        match (candidate.end, range.end) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(candidate_end), Some(range_end)) => candidate_end >= range_end,
        }
    })
}
