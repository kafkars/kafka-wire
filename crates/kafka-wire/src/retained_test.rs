//! Retained-size scenarios for nested buffers and tagged-field capacity.

use kafka_wire_core::{Bytes, TaggedField, TaggedFields};

use super::{RetainedFootprint, RetainedSize};

#[test]
fn nested_vector_counts_reserved_elements_and_visible_byte_storage() {
    let mut values = Vec::with_capacity(8);
    values.push(Some(Bytes::from_static(b"abc")));

    let retained = values.retained_size();

    assert_eq!(retained.heap_bytes(), 8 * size_of::<Option<Bytes>>() + 3);
    assert_eq!(retained.allocations(), 2);
}

#[test]
fn tagged_fields_count_reserved_entries_and_each_payload() {
    let mut fields = Vec::with_capacity(4);
    fields.push(TaggedField::new(1, Bytes::from_static(b"tag")));
    let fields = TaggedFields::from_sorted(fields)
        .unwrap_or_else(|error| panic!("ordered tags rejected: {error}"));

    let retained = fields.retained_size();

    assert_eq!(
        retained,
        RetainedFootprint::allocation(4 * size_of::<TaggedField>())
            .saturating_add(RetainedFootprint::allocation(3))
    );
}
