//! Recursive admission weight for generated protocol values.
//!
//! Container capacities are observable and charged exactly. Shared byte
//! buffers expose only their visible span, so that span and one bookkeeping
//! allocation are charged even when the backing store is shared or static.
//! This is a stable resource-policy input, not an estimate of process RSS.

use kafka_wire_core::{Bytes, StrBytes, TaggedFields, Uuid};

/// Accounted heap span and allocation count retained by one protocol value.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RetainedFootprint {
    heap_bytes: usize,
    allocations: usize,
}

impl RetainedFootprint {
    /// No separately allocated storage.
    pub const EMPTY: Self = Self {
        heap_bytes: 0,
        allocations: 0,
    };

    /// Charges one nonempty retained span and one bookkeeping allocation.
    pub const fn allocation(heap_bytes: usize) -> Self {
        Self {
            heap_bytes,
            allocations: if heap_bytes == 0 { 0 } else { 1 },
        }
    }

    /// Returns the accounted bytes held outside the inline Rust value.
    pub const fn heap_bytes(self) -> usize {
        self.heap_bytes
    }

    /// Returns separately accounted buffers retained by the value.
    pub const fn allocations(self) -> usize {
        self.allocations
    }

    /// Combines ownership without allowing accounting overflow to look small.
    #[must_use]
    pub const fn saturating_add(self, other: Self) -> Self {
        Self {
            heap_bytes: self.heap_bytes.saturating_add(other.heap_bytes),
            allocations: self.allocations.saturating_add(other.allocations),
        }
    }
}

/// Deep admission accounting for protocol DTO fields and containers.
pub trait RetainedSize {
    /// Returns container capacity plus recursively retained field spans.
    fn retained_size(&self) -> RetainedFootprint;
}

macro_rules! inline_only {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl RetainedSize for $ty {
                fn retained_size(&self) -> RetainedFootprint {
                    RetainedFootprint::EMPTY
                }
            }
        )+
    };
}

inline_only!(bool, i8, i16, i32, i64, u16, u32, f64, Uuid);

impl RetainedSize for Bytes {
    fn retained_size(&self) -> RetainedFootprint {
        RetainedFootprint::allocation(self.len())
    }
}

impl RetainedSize for StrBytes {
    fn retained_size(&self) -> RetainedFootprint {
        RetainedFootprint::allocation(self.len())
    }
}

impl RetainedSize for TaggedFields {
    fn retained_size(&self) -> RetainedFootprint {
        let fields = RetainedFootprint::allocation(
            self.capacity()
                .saturating_mul(size_of::<kafka_wire_core::TaggedField>()),
        );
        self.iter().fold(fields, |retained, field| {
            retained.saturating_add(field.data().retained_size())
        })
    }
}

impl<T> RetainedSize for Option<T>
where
    T: RetainedSize,
{
    fn retained_size(&self) -> RetainedFootprint {
        self.as_ref()
            .map_or(RetainedFootprint::EMPTY, RetainedSize::retained_size)
    }
}

impl<T> RetainedSize for Vec<T>
where
    T: RetainedSize,
{
    fn retained_size(&self) -> RetainedFootprint {
        let elements =
            RetainedFootprint::allocation(self.capacity().saturating_mul(size_of::<T>()));
        self.iter().fold(elements, |retained, value| {
            retained.saturating_add(value.retained_size())
        })
    }
}
