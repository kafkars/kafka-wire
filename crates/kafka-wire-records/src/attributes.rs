//! What a batch's attributes word means, bit by bit.
//!
//! Sixteen bits carry five independent facts, and the codec occupies only the
//! low three of them. Keeping the packing here rather than in `batch` means the
//! two directions cannot drift: a bit read in one place and written in another
//! is exactly the shape of defect this repository has already been bitten by.

use crate::error::RecordError;

/// How a batch's timestamps were assigned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimestampType {
    /// The producer stamped each record.
    CreateTime,
    /// The broker stamped the batch on append.
    LogAppendTime,
}

/// Which codec the records payload is compressed with.
///
/// The codec identity lives in the batch attributes while the header itself is
/// always uncompressed, which is what lets a broker route a batch it cannot
/// decompress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Compression {
    /// Records are stored verbatim.
    None,
    /// gzip, as `DEFLATE` in a gzip wrapper.
    Gzip,
    /// snappy in the xerial FRAMED format, whose payload opens with a `0x82`
    /// `SNAPPY` magic. Not raw snappy, which is what an implementation reaching
    /// for the obvious library produces.
    Snappy,
    /// LZ4 in the frame format, opening with the `0x04224D18` frame magic.
    Lz4,
    /// zstd, opening with the `0x28B52FFD` frame magic.
    Zstd,
}

impl Compression {
    pub(crate) const fn from_bits(bits: u8) -> Result<Self, RecordError> {
        match bits {
            0 => Ok(Self::None),
            1 => Ok(Self::Gzip),
            2 => Ok(Self::Snappy),
            3 => Ok(Self::Lz4),
            4 => Ok(Self::Zstd),
            codec => Err(RecordError::UnknownCompression { codec }),
        }
    }

    pub(crate) const fn bits(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Gzip => 1,
            Self::Snappy => 2,
            Self::Lz4 => 3,
            Self::Zstd => 4,
        }
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::None => "uncompressed",
            Self::Gzip => "gzip",
            Self::Snappy => "snappy",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }
}

/// The five facts a batch's attributes word carries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Attributes {
    pub(crate) compression: Compression,
    pub(crate) timestamp_type: TimestampType,
    pub(crate) is_transactional: bool,
    pub(crate) is_control: bool,
    pub(crate) has_delete_horizon: bool,
}

impl Attributes {
    /// Bit 3 is the timestamp type, 4 transactional, 5 control, 6 delete horizon.
    const TIMESTAMP_TYPE: i16 = 0x08;
    const TRANSACTIONAL: i16 = 0x10;
    const CONTROL: i16 = 0x20;
    const DELETE_HORIZON: i16 = 0x40;
    const CODEC: i16 = 0x07;

    pub(crate) fn decode(bits: i16) -> Result<Self, RecordError> {
        Ok(Self {
            compression: Compression::from_bits(u8::try_from(bits & Self::CODEC).unwrap_or(0))?,
            timestamp_type: if bits & Self::TIMESTAMP_TYPE == 0 {
                TimestampType::CreateTime
            } else {
                TimestampType::LogAppendTime
            },
            is_transactional: bits & Self::TRANSACTIONAL != 0,
            is_control: bits & Self::CONTROL != 0,
            has_delete_horizon: bits & Self::DELETE_HORIZON != 0,
        })
    }

    pub(crate) fn encode(self) -> i16 {
        let mut bits = i16::from(self.compression.bits());
        if self.timestamp_type == TimestampType::LogAppendTime {
            bits |= Self::TIMESTAMP_TYPE;
        }
        if self.is_transactional {
            bits |= Self::TRANSACTIONAL;
        }
        if self.is_control {
            bits |= Self::CONTROL;
        }
        if self.has_delete_horizon {
            bits |= Self::DELETE_HORIZON;
        }
        bits
    }
}
