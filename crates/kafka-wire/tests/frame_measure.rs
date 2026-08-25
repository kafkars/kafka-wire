//! Scenario: transport capacity is reserved from an exact request measurement.

use kafka_wire as wire;
use kafka_wire_core as core;

const TEST_LIMITS: wire::OutboundFrameLimits = wire::OutboundFrameLimits::new(1024);

fn api_versions_request() -> wire::ApiVersionsRequest {
    let mut request = wire::ApiVersionsRequest::default();
    request.client_software_name = core::StrBytes::from("acme");
    request.client_software_version = core::StrBytes::from("1.0");
    request
}

#[test]
fn measurement_matches_the_complete_encoded_frame() {
    let request = api_versions_request();
    let measure = wire::measure_request(&request, core::ApiVersion::new(3), None, TEST_LIMITS)
        .unwrap_or_else(|error| panic!("v3 is supported: {error}"));
    let mut buffer = bytes::BytesMut::new();
    let written = wire::encode_request(
        &mut buffer,
        7,
        None,
        &request,
        core::ApiVersion::new(3),
        TEST_LIMITS,
    )
    .unwrap_or_else(|error| panic!("v3 is supported: {error}"));

    assert_eq!(
        measure,
        wire::RequestFrameMeasure {
            wire_bytes: written,
            response_header_version: core::ApiVersion::new(0),
        }
    );
    assert_eq!(measure.wire_bytes, buffer.len());
}

#[test]
fn measurement_accounts_for_client_id_and_flexible_response_headers() {
    let client_id = core::StrBytes::from("driver");
    let api_versions = api_versions_request();
    let without_client =
        wire::measure_request(&api_versions, core::ApiVersion::new(3), None, TEST_LIMITS)
            .unwrap_or_else(|error| panic!("v3 is supported: {error}"));
    let with_client = wire::measure_request(
        &api_versions,
        core::ApiVersion::new(3),
        Some(&client_id),
        TEST_LIMITS,
    )
    .unwrap_or_else(|error| panic!("v3 is supported: {error}"));
    assert_eq!(
        with_client.wire_bytes,
        without_client.wire_bytes + client_id.len()
    );

    let metadata = wire::MetadataRequest::default();
    let metadata_measure = wire::measure_request(
        &metadata,
        core::ApiVersion::new(9),
        Some(&client_id),
        TEST_LIMITS,
    )
    .unwrap_or_else(|error| panic!("v9 is supported: {error}"));
    let mut buffer = bytes::BytesMut::new();
    let written = wire::encode_request(
        &mut buffer,
        19,
        Some(client_id),
        &metadata,
        core::ApiVersion::new(9),
        TEST_LIMITS,
    )
    .unwrap_or_else(|error| panic!("v9 is supported: {error}"));

    assert_eq!(metadata_measure.wire_bytes, written);
    assert_eq!(
        metadata_measure.response_header_version,
        core::ApiVersion::new(1)
    );
}

#[test]
fn measurement_and_encoding_reject_the_same_preflight_failures() {
    let request = api_versions_request();
    let limits = wire::OutboundFrameLimits::new(0);
    let Err(measured_error) =
        wire::measure_request(&request, core::ApiVersion::new(3), None, limits)
    else {
        panic!("a zero-byte frame limit must reject this request")
    };
    let Err(encoded_error) = wire::encode_request(
        &mut bytes::BytesMut::new(),
        7,
        None,
        &request,
        core::ApiVersion::new(3),
        limits,
    ) else {
        panic!("encoding must enforce the same zero-byte frame limit")
    };
    assert_eq!(measured_error, encoded_error);
    assert!(matches!(
        measured_error,
        core::EncodeError::FrameLimitExceeded { limit: 0, .. }
    ));

    assert_eq!(
        wire::measure_request(&request, core::ApiVersion::new(99), None, TEST_LIMITS),
        Err(core::EncodeError::UnsupportedVersion {
            message: "ApiVersionsRequest",
            version: core::ApiVersion::new(99),
            supported: <wire::ApiVersionsRequest as wire::KafkaMessage>::SUPPORTED_VERSIONS,
        })
    );
}
