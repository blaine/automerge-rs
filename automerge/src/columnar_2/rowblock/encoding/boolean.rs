use std::borrow::{Cow, Borrow};

use super::{Encodable, RawDecoder, Source, Sink};

/// Encodes booleans by storing the count of the same value.
///
/// The sequence of numbers describes the count of false values on even indices (0-indexed) and the
/// count of true values on odd indices (0-indexed).
///
/// Counts are encoded as usize.
pub(crate) struct BooleanEncoder<'a> {
    written: usize,
    buf: &'a mut Vec<u8>,
    last: bool,
    count: usize,
}

impl<'a> BooleanEncoder<'a> {
    pub fn new(output: &'a mut Vec<u8>) -> BooleanEncoder<'a> {
        BooleanEncoder {
            written: 0,
            buf: output,
            last: false,
            count: 0,
        }
    }

    pub fn append(&mut self, value: bool) {
        if value == self.last {
            self.count += 1;
        } else {
            self.written += self.count.encode(&mut self.buf);
            self.last = value;
            self.count = 1;
        }
    }

    pub fn finish(mut self) -> usize {
        if self.count > 0 {
            self.written += self.count.encode(&mut self.buf);
        }
        self.written
    }
}

impl<'a> From<&'a mut Vec<u8>> for BooleanEncoder<'a> {
    fn from(output: &'a mut Vec<u8>) -> Self {
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

impl<'a> From<Cow<'a, [u8]>> for BooleanDecoder<'a> {
    fn from(bytes: Cow<'a, [u8]>) -> Self {
        BooleanDecoder {
            decoder: RawDecoder::from(bytes),
            last_value: true,
            count: 0,
        }
    }
}

impl<'a> From<&'a [u8]> for BooleanDecoder<'a> {
    fn from(d: &'a [u8]) -> Self {
        Cow::Borrowed(d).into() 
    }
}

// this is an endless iterator that returns false after input is exhausted
impl<'a> Iterator for BooleanDecoder<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<bool> {
        while self.count == 0 {
            if self.decoder.done() && self.count == 0 {
                return None;
            }
            self.count = self.decoder.read().unwrap_or_default();
            self.last_value = !self.last_value;
        }
        self.count -= 1;
        Some(self.last_value)
    }
}

impl<'a, 'b> Source for &'b mut BooleanDecoder<'a> {
    fn done(&self) -> bool {
        BooleanDecoder::done(self)
    }
}

impl<'a> Sink for BooleanEncoder<'a> {
    type Item = bool;

    fn append<I: Borrow<Self::Item>>(&mut self, item: Option<I>) {
        match item {
            Some(b) => BooleanEncoder::append(self, *b.borrow()),
            None => BooleanEncoder::append(self, false),
        }
    }

    fn finish(self) -> usize {
        BooleanEncoder::finish(self)
    }
}
