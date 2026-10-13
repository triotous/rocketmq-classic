use std::io;

use rocketmq_classic_common::{compression, sysflag::message::CompressionType};

/// The compression backend snapshot returned by [`crate::Producer::compressor`].
///
/// Like Java's `Compressor`, this value exposes compression and decompression
/// operations. A value remains bound to the type it was created with if the
/// producer's compression type changes later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerCompressor {
    compression_type: CompressionType,
}

impl ProducerCompressor {
    pub(crate) const fn new(compression_type: CompressionType) -> Self {
        Self { compression_type }
    }

    /// Returns the RocketMQ compression type used by this backend.
    pub const fn compression_type(self) -> CompressionType {
        self.compression_type
    }

    /// Compresses bytes with the supplied RocketMQ compression level.
    pub fn compress(&self, source: &[u8], level: i32) -> io::Result<Vec<u8>> {
        compression::compress(self.compression_type, source, level)
    }

    /// Decompresses bytes produced by this RocketMQ compression type.
    pub fn decompress(&self, source: &[u8]) -> io::Result<Vec<u8>> {
        compression::decompress(self.compression_type, source)
    }
}
