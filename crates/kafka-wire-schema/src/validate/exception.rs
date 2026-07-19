//! Documented upstream schema defects a caller chooses to accept.
//!
//! This file owns the vocabulary for "this named message violates this named
//! invariant, on purpose, and here is the upstream reference". It deliberately
//! does not own where that list comes from: the data lives in `spec/overrides/`
//! and reaches this crate as a value, because a validator that reads its own
//! exemptions from disk can no longer be reasoned about from its inputs.

use super::ValidationError;

/// One accepted violation of one invariant by one upstream message.
///
/// An exception is deliberately specific — message, code, and field — so that
/// accepting a known defect in `DescribeConfigsResponse.Documentation` cannot
/// also silence the same class of defect somewhere nobody looked.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaException {
    /// Protocol message name the exception applies to.
    pub message: String,
    /// Protocol field name, or `None` for a message-level diagnostic.
    pub field: Option<String>,
    /// The stable diagnostic code being accepted.
    pub code: String,
    /// Why the defect is tolerable rather than fixed.
    pub reason: String,
    /// Upstream file or KIP the defect can be traced to.
    pub upstream: String,
}

impl SchemaException {
    /// Returns whether this exception covers `error` raised by `message`.
    fn covers(&self, message: &str, error: &ValidationError) -> bool {
        self.message == message
            && self.code == error.code
            && self.field.as_deref() == error.field.as_deref()
    }
}

/// The set of upstream defects a validation run is permitted to ignore.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SchemaExceptions {
    accepted: Vec<SchemaException>,
}

impl SchemaExceptions {
    /// Builds an exception set from reviewed override data.
    pub fn new(accepted: Vec<SchemaException>) -> Self {
        Self { accepted }
    }

    /// Returns the empty set, under which every invariant is enforced.
    pub const fn none() -> Self {
        Self {
            accepted: Vec::new(),
        }
    }

    /// Returns the accepted entries in declaration order.
    pub fn entries(&self) -> &[SchemaException] {
        &self.accepted
    }

    /// Returns whether `error` from `message` is a documented exception.
    pub(super) fn accepts(&self, message: &str, error: &ValidationError) -> bool {
        self.accepted
            .iter()
            .any(|exception| exception.covers(message, error))
    }
}
