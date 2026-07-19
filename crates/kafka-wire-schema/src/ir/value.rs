//! Typed protocol defaults after source lowering.

/// Default value used when a field is absent from a wire version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DefaultValue {
    /// Null default.
    Null,
    /// Boolean default.
    Bool(bool),
    /// Signed integer default.
    Integer(i64),
    /// IEEE 754 double default.
    Float(FloatDefault),
    /// String default.
    String(String),
    /// UUID default, as sixteen big-endian bytes.
    Uuid([u8; 16]),
    /// Empty array or byte sequence.
    Empty,
    /// A struct with every member at its own default.
    ///
    /// The implicit default of a non-nullable struct field. It is not `Null`:
    /// `FetchResponse.CurrentLeader` is absent from a v11 response and arrives
    /// as leader `-1`, epoch `-1` — a real struct that says "unknown" — not as
    /// the absence of a struct, which that field cannot encode.
    StructDefaults,
}

/// A float default compared by its exact bit pattern.
///
/// Two schema declarations are the same declaration when they were written the
/// same way. IEEE equality answers a different question — it makes `NaN`
/// unequal to itself and `-0.0` equal to `0.0` — so it is the wrong relation
/// for deciding whether two lowered schemas agree.
#[derive(Clone, Copy, Debug)]
pub struct FloatDefault(f64);

impl FloatDefault {
    /// Wraps one protocol float default.
    pub const fn new(value: f64) -> Self {
        Self(value)
    }

    /// Returns the underlying double.
    pub const fn get(self) -> f64 {
        self.0
    }
}

impl PartialEq for FloatDefault {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for FloatDefault {}
