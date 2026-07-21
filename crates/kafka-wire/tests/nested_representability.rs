//! Nested values fail before a selected version can silently discard their fields.
//!
//! Scenario: exercise the concrete `UpdateFeatures` evolution boundary both by
//! encoding its public nested structure directly and through its request owner.

use bytes::Bytes;
use kafka_wire::{
    UpdateFeaturesRequest, delete_topics_request::DeleteTopicState,
    update_features_request::FeatureUpdateKey,
};
use kafka_wire_core::{
    ApiVersion, DecodeError, DecodeLimits, EncodeError, KafkaDecode, KafkaEncode, VersionRange,
};

#[test]
fn version_one_refuses_the_retired_allow_downgrade_field() {
    let mut update = FeatureUpdateKey::default();
    update.allow_downgrade = true;

    assert_eq!(
        update.encode_to_bytes(ApiVersion::new(1)),
        Err(EncodeError::FieldNotRepresentable {
            message: "FeatureUpdateKey",
            field: "AllowDowngrade",
            version: ApiVersion::new(1),
        })
    );
}

#[test]
fn version_zero_refuses_the_later_upgrade_type_field() {
    let mut update = FeatureUpdateKey::default();
    update.upgrade_type = 2;

    assert_eq!(
        update.encode_to_bytes(ApiVersion::new(0)),
        Err(EncodeError::FieldNotRepresentable {
            message: "FeatureUpdateKey",
            field: "UpgradeType",
            version: ApiVersion::new(0),
        })
    );
}

#[test]
fn owner_encoding_recursively_validates_each_nested_value() {
    let mut update = FeatureUpdateKey::default();
    update.allow_downgrade = true;
    let mut request = UpdateFeaturesRequest::default();
    request.feature_updates.push(update);

    assert_eq!(
        request.encode_to_bytes(ApiVersion::new(1)),
        Err(EncodeError::FieldNotRepresentable {
            message: "FeatureUpdateKey",
            field: "AllowDowngrade",
            version: ApiVersion::new(1),
        })
    );
}

#[test]
fn direct_nested_encoding_rejects_versions_its_owner_does_not_support() {
    assert_eq!(
        FeatureUpdateKey::default().encode_to_bytes(ApiVersion::new(3)),
        Err(EncodeError::UnsupportedVersion {
            message: "FeatureUpdateKey",
            version: ApiVersion::new(3),
            supported: VersionRange::new(0, 2),
        })
    );
}

#[test]
fn direct_nested_decoding_rejects_versions_its_owner_does_not_support() {
    assert_eq!(
        FeatureUpdateKey::decode_from_bytes(
            Bytes::new(),
            ApiVersion::new(3),
            DecodeLimits::default(),
        ),
        Err(DecodeError::UnsupportedVersion {
            message: "FeatureUpdateKey",
            version: ApiVersion::new(3),
            supported: VersionRange::new(0, 2),
        })
    );
}

#[test]
fn direct_nested_encoding_uses_the_declarations_narrower_range() {
    assert_eq!(
        DeleteTopicState::default().encode_to_bytes(ApiVersion::new(1)),
        Err(EncodeError::UnsupportedVersion {
            message: "DeleteTopicState",
            version: ApiVersion::new(1),
            supported: VersionRange::new(6, 6),
        })
    );
}

#[test]
fn direct_nested_decoding_uses_the_declarations_narrower_range() {
    assert_eq!(
        DeleteTopicState::decode_from_bytes(
            Bytes::new(),
            ApiVersion::new(1),
            DecodeLimits::default(),
        ),
        Err(DecodeError::UnsupportedVersion {
            message: "DeleteTopicState",
            version: ApiVersion::new(1),
            supported: VersionRange::new(6, 6),
        })
    );
}
