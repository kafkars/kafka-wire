//! One buffer, many messages: each encode reports only the bytes it wrote.
//!
//! A client that pipelines writes a size prefix, a header, and a body into one
//! reused `BytesMut`. These stories pin that the encoder's length, and the
//! predicted-versus-written self-check built on it, describe the current
//! message rather than everything the buffer happens to hold.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use bytes::BytesMut;
use kafka_wire_core::{ApiVersion, EncodeError, EncodeTarget, Encoder, KafkaEncode, StrBytes};

const VERSION: ApiVersion = ApiVersion::new(0);

/// Bytes an earlier frame already left in the shared buffer.
const EARLIER_FRAME: &[u8] = b"\x00\x00\x00\x0f";

/// One earlier byte plus a four-byte message makes five: exactly the size
/// `MisreportedSize` claims. A whole-buffer length reads that coincidence as
/// agreement and lets the divergence through.
const MASKING_PREFIX: &[u8] = b"\x00";

/// A fifteen-byte message: one `int16` and one legacy string.
#[derive(Debug)]
struct Coordinator {
    host: StrBytes,
}

impl Coordinator {
    fn new() -> Self {
        Self {
            host: StrBytes::from("coordinator"),
        }
    }
}

impl KafkaEncode for Coordinator {
    fn encode<T: EncodeTarget>(
        &self,
        encoder: &mut Encoder<T>,
        _version: ApiVersion,
    ) -> Result<(), EncodeError> {
        encoder.write_i16(7)?;
        encoder.write_string(&self.host)
    }
}

/// A message that declares one more byte than it writes.
///
/// Real generated code cannot diverge this way, which is exactly why the
/// self-check needs a deliberate counterexample to stay honest.
#[derive(Debug)]
struct MisreportedSize;

impl KafkaEncode for MisreportedSize {
    fn encode<T: EncodeTarget>(
        &self,
        encoder: &mut Encoder<T>,
        _version: ApiVersion,
    ) -> Result<(), EncodeError> {
        encoder.write_i32(0)
    }

    fn encoded_len(&self, _version: ApiVersion) -> Result<usize, EncodeError> {
        Ok(5)
    }
}

#[test]
fn encoder_length_excludes_bytes_the_buffer_already_held() {
    let mut buffer = BytesMut::from(EARLIER_FRAME);

    let mut encoder = Encoder::new(&mut buffer);
    encoder.write_i32(1).unwrap();

    assert_eq!(encoder.len(), 4);
    assert!(!encoder.is_empty());
    assert_eq!(buffer.len(), EARLIER_FRAME.len() + 4);
}

#[test]
fn a_fresh_encoder_over_a_used_buffer_is_empty() {
    let mut buffer = BytesMut::from(EARLIER_FRAME);

    let encoder = Encoder::new(&mut buffer);

    assert_eq!(encoder.len(), 0);
    assert!(encoder.is_empty());
}

#[test]
fn the_size_self_check_is_not_masked_by_an_earlier_frame() {
    let mut buffer = BytesMut::from(MASKING_PREFIX);
    let predicted = MisreportedSize.encoded_len(VERSION).unwrap();

    let mut encoder = Encoder::new(&mut buffer);
    MisreportedSize.encode(&mut encoder, VERSION).unwrap();

    assert_eq!(encoder.len(), 4);
    assert_ne!(
        encoder.len(),
        predicted,
        "a one-byte divergence was masked by the bytes already in the buffer"
    );
}

#[test]
fn three_messages_share_one_buffer_and_report_their_own_lengths() {
    let message = Coordinator::new();
    let mut buffer = BytesMut::new();

    let mut written = Vec::new();
    for _ in 0..3 {
        written.push(message.encode_into(&mut buffer, VERSION).unwrap());
    }

    assert_eq!(written, vec![15, 15, 15]);
    assert_eq!(buffer.len(), 45);
    assert_eq!(&buffer[..15], &buffer[15..30]);
    assert_eq!(&buffer[15..30], &buffer[30..]);
}

#[test]
fn encoding_into_a_reused_buffer_still_detects_a_size_mismatch() {
    let mut buffer = BytesMut::from(MASKING_PREFIX);

    let error = MisreportedSize
        .encode_into(&mut buffer, VERSION)
        .unwrap_err();

    assert_eq!(
        error,
        EncodeError::SizeMismatch {
            predicted: 5,
            actual: 4,
        }
    );
}

#[test]
fn a_rejected_message_leaves_no_partial_bytes_behind() {
    let mut buffer = BytesMut::new();
    let accepted = Coordinator::new()
        .encode_into(&mut buffer, VERSION)
        .unwrap();

    MisreportedSize
        .encode_into(&mut buffer, VERSION)
        .unwrap_err();

    assert_eq!(buffer.len(), accepted);
}

#[test]
fn encoding_to_bytes_agrees_with_encoding_into_a_buffer() {
    let message = Coordinator::new();
    let mut buffer = BytesMut::new();

    let written = message.encode_into(&mut buffer, VERSION).unwrap();
    let bytes = message.encode_to_bytes(VERSION).unwrap();

    assert_eq!(written, bytes.len());
    assert_eq!(buffer.as_ref(), bytes.as_ref());
}
