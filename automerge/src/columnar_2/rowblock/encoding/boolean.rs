use std::borrow::Cow;

use super::{Encodable, RawDecoder};

/// Encodes booleans by storing the count of the same value.
///
/// The sequence of numbers describes the count of false values on even indices (0-indexed) and the
/// count of true values on odd indices (0-indexed).
///
/// Counts are encoded as usize.
pub(crate) struct BooleanEncoder<'a> {
    start_len: usize,
    buf: &'a mut [u8],
    last: bool,
    count: usize,
}

impl<'a> BooleanEncoder<'a> {
    pub fn new(output: &'a mut [u8]) -> BooleanEncoder<'a> {
        BooleanEncoder {
            start_len: output.len(),
            buf: output,
            last: false,
            count: 0,
        }
    }

    pub fn append(&mut self, value: bool) {
        if value == self.last {
            self.count += 1;
        } else {
            self.count.encode(&mut self.buf);
            self.last = value;
            self.count = 1;
        }
    }

    pub fn finish(mut self) -> usize {
        if self.count > 0 {
            self.count.encode(&mut self.buf);
        }
        self.start_len - self.buf.len()
    }
}

impl<'a> From<&'a mut [u8]> for BooleanEncoder<'a> {
    fn from(output: &'a mut [u8]) -> Self {
        BooleanEncoder::new(output)
    }
}

/// See the discussion of [`BooleanEncoder`] for details on this encoding
pub(crate) struct BooleanDecoder<'a> {
    decoder: RawDecoder<'a>,
    last_value: bool,
    count: usize,
}

impl<'a> BooleanDecoder<'a> {
    pub(crate) fn done(&self) -> bool {
        self.decoder.done()
    }
}

impl<'a> From<Cow<'a, [u8]>> for RawDecoder<'a> {
    fn from(bytes: Cow<'a, [u8]>) -> RawDecoder<'a> {
        RawDecoder::new(bytes)
    }
}

impl<'a> From<Cow<'a, [u8]>> for BooleanDecoder<'a> {
    fn from(bytes: Cow<'a, [u8]>) -> Self {
        BooleanDecoder {
            decoder: RawDecoder::from(bytes),
            last_value: true,
            count: 0,
        }
    }
}

// this is an endless iterator that returns false after input is exhausted
impl<'a> Iterator for BooleanDecoder<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<bool> {
        while self.count == 0 {
            if self.decoder.done() && self.count == 0 {
                return Some(false);
            }
            self.count = self.decoder.read().unwrap_or_default();
            self.last_value = !self.last_value;
        }
        self.count -= 1;
        Some(self.last_value)
    }
}

