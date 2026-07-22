//! Bounded streaming compression into the final record-batch buffer.
//!
//! This file owns outbound codec mechanics and the cap enforced while codecs
//! produce bytes. It does not choose batch limits or frame header fields.

use std::{cell::Cell, io::Write, rc::Rc};

use bytes::BytesMut;

use crate::{
    attributes::Compression,
    compression::{XERIAL_MAGIC, xerial_block_length},
    error::RecordError,
};

const SNAPPY_BLOCK_BYTES: usize = 32 * 1024;

impl Compression {
    /// Compresses `records` directly into `output` without crossing the batch cap.
    pub(crate) fn compress_into(
        self,
        records: &[u8],
        output: &mut BytesMut,
        batch_start: usize,
        max_batch_bytes: usize,
    ) -> Result<(), RecordError> {
        match self {
            Self::None => unreachable!("uncompressed records bypass compression"),
            Self::Gzip => stream_gzip(records, output, batch_start, max_batch_bytes),
            Self::Snappy => stream_xerial(records, output, batch_start, max_batch_bytes),
            Self::Lz4 => stream_lz4(records, output, batch_start, max_batch_bytes),
            Self::Zstd => stream_zstd(records, output, batch_start, max_batch_bytes),
        }
    }
}

fn stream_gzip(
    records: &[u8],
    output: &mut BytesMut,
    start: usize,
    limit: usize,
) -> Result<(), RecordError> {
    let (mut writer, overflow) = BoundedWriter::new(output, start, limit);
    let result = (|| -> Result<(), String> {
        let mut encoder =
            flate2::write::GzEncoder::new(&mut writer, flate2::Compression::default());
        encoder
            .write_all(records)
            .map_err(|error| error.to_string())?;
        encoder.finish().map_err(|error| error.to_string())?;
        Ok(())
    })();
    compression_result("gzip", result, &overflow, limit)
}

fn stream_lz4(
    records: &[u8],
    output: &mut BytesMut,
    start: usize,
    limit: usize,
) -> Result<(), RecordError> {
    let (mut writer, overflow) = BoundedWriter::new(output, start, limit);
    let result = (|| -> Result<(), String> {
        let mut encoder = lz4_flex::frame::FrameEncoder::new(&mut writer);
        encoder
            .write_all(records)
            .map_err(|error| error.to_string())?;
        encoder.finish().map_err(|error| error.to_string())?;
        Ok(())
    })();
    compression_result("lz4", result, &overflow, limit)
}

fn stream_zstd(
    records: &[u8],
    output: &mut BytesMut,
    start: usize,
    limit: usize,
) -> Result<(), RecordError> {
    let (mut writer, overflow) = BoundedWriter::new(output, start, limit);
    let result = (|| -> Result<(), String> {
        let mut encoder =
            zstd::stream::write::Encoder::new(&mut writer, 3).map_err(|error| error.to_string())?;
        encoder
            .write_all(records)
            .map_err(|error| error.to_string())?;
        encoder.finish().map_err(|error| error.to_string())?;
        Ok(())
    })();
    compression_result("zstd", result, &overflow, limit)
}

fn stream_xerial(
    records: &[u8],
    output: &mut BytesMut,
    start: usize,
    limit: usize,
) -> Result<(), RecordError> {
    let (mut writer, _overflow) = BoundedWriter::new(output, start, limit);
    writer.append(&XERIAL_MAGIC)?;
    writer.append(&1_i32.to_be_bytes())?;
    writer.append(&1_i32.to_be_bytes())?;
    for plain in records.chunks(SNAPPY_BLOCK_BYTES) {
        let block = snap::raw::Encoder::new()
            .compress_vec(plain)
            .map_err(|error| Compression::failed("snappy", &error))?;
        writer.append(&xerial_block_length(block.len())?.to_be_bytes())?;
        writer.append(&block)?;
    }
    Ok(())
}

fn compression_result(
    codec: &'static str,
    result: Result<(), String>,
    overflow: &Cell<usize>,
    limit: usize,
) -> Result<(), RecordError> {
    match (result, overflow.get()) {
        (_, length) if length != 0 => Err(RecordError::BatchLimitExceeded { length, limit }),
        (Ok(()), _) => Ok(()),
        (Err(detail), _) => Err(RecordError::CompressionFailed { codec, detail }),
    }
}

struct BoundedWriter<'a> {
    output: &'a mut BytesMut,
    batch_start: usize,
    limit: usize,
    overflow: Rc<Cell<usize>>,
}

impl<'a> BoundedWriter<'a> {
    fn new(output: &'a mut BytesMut, batch_start: usize, limit: usize) -> (Self, Rc<Cell<usize>>) {
        let overflow = Rc::new(Cell::new(0));
        (
            Self {
                output,
                batch_start,
                limit,
                overflow: Rc::clone(&overflow),
            },
            overflow,
        )
    }

    fn append(&mut self, bytes: &[u8]) -> Result<(), RecordError> {
        let current = self.output.len().saturating_sub(self.batch_start);
        let length = current.checked_add(bytes.len()).unwrap_or(usize::MAX);
        if length > self.limit {
            self.overflow.set(length);
            return Err(RecordError::BatchLimitExceeded {
                length,
                limit: self.limit,
            });
        }
        self.output.extend_from_slice(bytes);
        Ok(())
    }
}

impl Write for BoundedWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.append(buffer)
            .map(|()| buffer.len())
            // Keep the concrete cause in `overflow`. `lz4_flex` attempts to
            // downcast any boxed `io::Error` payload to its own error type, so
            // the transport error must carry no boxed source.
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::WriteZero))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
