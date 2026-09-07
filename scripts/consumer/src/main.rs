use kafka_wire::{
    ApiVersionsRequest, OutboundFrameLimits, ProtocolEq, RequestHeader, encode_request,
    measure_request,
};
use kafka_wire_core::{
    ApiVersion, Bytes, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode, StrBytes,
};
use kafka_wire_records::{
    Compression, Record, RecordBatch, RecordBatchDecode, RecordDecodeLimits, RecordEncodeLimits,
    RecordError, RecordHeader, TimestampType,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    request_frame()?;
    for compression in [
        Compression::None,
        Compression::Gzip,
        Compression::Lz4,
        Compression::Snappy,
        Compression::Zstd,
    ] {
        record_batch(compression)?;
    }
    println!("qualified independent wire consumer: framing, limits, messages, and all codecs");
    Ok(())
}

fn request_frame() -> Result<(), Box<dyn std::error::Error>> {
    let version = ApiVersion::new(3);
    let mut request = ApiVersionsRequest::default();
    request.client_software_name = StrBytes::from("release-consumer");
    request.client_software_version = StrBytes::from("0.1.0");
    let limit = OutboundFrameLimits::new(1024);
    let measure = measure_request(&request, version, None, limit)?;
    let mut frame = BytesMut::new();
    assert_eq!(
        encode_request(&mut frame, 42, None, &request, version, limit)?,
        measure.wire_bytes
    );
    let mut decoder = Decoder::new(frame.freeze(), DecodeLimits::default())?;
    assert_eq!(
        usize::try_from(decoder.read_i32()?)?,
        measure.wire_bytes - 4
    );
    let header = RequestHeader::decode(&mut decoder, ApiVersion::new(2))?;
    assert_eq!(header.correlation_id, 42);
    assert_eq!(header.request_api_version, 3);
    let decoded = ApiVersionsRequest::decode(&mut decoder, version)?;
    assert!(request.protocol_eq(&decoded));
    decoder.finish()?;

    let body = request.encode_to_bytes(version)?;
    let mut limits = DecodeLimits::default();
    limits.max_frame_bytes = body.len() - 1;
    assert!(ApiVersionsRequest::decode_from_bytes(body.clone(), version, limits).is_err());
    let mut trailing = BytesMut::from(body.as_ref());
    trailing.extend_from_slice(&[0]);
    assert!(
        ApiVersionsRequest::decode_from_bytes(trailing.freeze(), version, DecodeLimits::default())
            .is_err()
    );
    let mut destination = BytesMut::from(&b"earlier-frame"[..]);
    let before = destination.clone();
    assert!(
        encode_request(
            &mut destination,
            42,
            None,
            &request,
            version,
            OutboundFrameLimits::new(0)
        )
        .is_err()
    );
    assert_eq!(destination, before);
    assert!(measure_request(&request, ApiVersion::new(-1), None, limit).is_err());
    Ok(())
}

fn record_batch(compression: Compression) -> Result<(), Box<dyn std::error::Error>> {
    let batch = RecordBatch {
        base_offset: 0,
        last_offset_delta: 2,
        partition_leader_epoch: -1,
        compression,
        timestamp_type: TimestampType::CreateTime,
        is_transactional: false,
        is_control: false,
        has_delete_horizon: false,
        base_timestamp: 0,
        max_timestamp: 0,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: [Some(Bytes::from_static(b"value")), None, Some(Bytes::new())]
            .into_iter()
            .zip(0..3)
            .map(|(value, offset_delta)| Record {
                attributes: 0,
                timestamp_delta: 0,
                offset_delta,
                key: Some(Bytes::from_static(b"key")),
                value,
                headers: vec![RecordHeader {
                    key: StrBytes::from("header"),
                    value: None,
                }],
            })
            .collect(),
    };
    let bytes = batch.encode_to_bytes(RecordEncodeLimits::new(4096, 4096))?;
    // The default expanded-payload budget also admits the encoder's Zstd window.
    let limits = RecordDecodeLimits::new(
        4096,
        RecordDecodeLimits::default().max_decompressed_records_bytes,
        DecodeLimits::default(),
    );
    if compression == Compression::Zstd {
        let mut cursor = bytes.clone();
        let tight = RecordDecodeLimits::new(4096, 4096, DecodeLimits::default());
        assert!(matches!(
            RecordBatch::decode(&mut cursor, tight),
            Err(RecordError::CompressionFailed { codec: "zstd", .. })
        ));
        assert_eq!(cursor, bytes);
    }
    let mut cursor = bytes.clone();
    assert_eq!(RecordBatch::decode(&mut cursor, limits)?, batch);
    assert!(cursor.is_empty());
    let mut cursor = bytes.slice(..bytes.len() - 1);
    let before = cursor.clone();
    assert!(matches!(
        RecordBatch::decode_next(&mut cursor, limits, 4096)?,
        RecordBatchDecode::PartialTrailing { .. }
    ));
    assert_eq!(cursor, before);
    assert!(RecordBatch::decode(&mut cursor, limits).is_err());
    assert_eq!(cursor, before);
    let mut corrupt = bytes.to_vec();
    corrupt[21] ^= 1;
    let mut cursor = Bytes::from(corrupt);
    let before = cursor.clone();
    assert!(RecordBatch::decode(&mut cursor, limits).is_err());
    assert_eq!(cursor, before);
    let mut destination = BytesMut::from(&b"earlier-batch"[..]);
    let before = destination.clone();
    assert!(
        batch
            .encode_into(&mut destination, RecordEncodeLimits::new(4096, 0))
            .is_err()
    );
    assert_eq!(destination, before);
    Ok(())
}
