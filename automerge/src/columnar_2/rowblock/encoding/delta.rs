use std::borrow::Cow;

use super::{RleEncoder, RleDecoder, Source, Sink};

/// Encodes integers as the change since the previous value.
///
/// The initial value is 0 encoded as u64. Deltas are encoded as i64.
///
/// Run length encoding is then applied to the resulting sequence.
pub(crate) struct DeltaEncoder<'a> {
    rle: RleEncoder<'a, i64>,
    absolute_value: u64,
}

impl<'a> DeltaEncoder<'a> {
    pub fn new(output: &'a mut Vec<u8>) -> DeltaEncoder<'a> {
        DeltaEncoder {
            rle: RleEncoder::new(output),
            absolute_value: 0,
        }
    }

    pub fn append_value(&mut self, value: u64) {
        self.rle
            .append_value(value as i64 - self.absolute_value as i64);
        self.absolute_value = value;
    }

    pub fn append_null(&mut self) {
        self.rle.append_null();
    }

    pub fn finish(self) -> usize {
        self.rle.finish()
    }
}

impl<'a> From<&'a mut Vec<u8>> for DeltaEncoder<'a> {
    fn from(output: &'a mut Vec<u8>) -> Self {
        DeltaEncoder::new(output) 
    }
}

/// See discussion on [`DeltaEncoder`] for the format data is stored in.
pub(crate) struct DeltaDecoder<'a> {
    rle: RleDecoder<'a, i64>,
    absolute_val: u64,
}

impl<'a> DeltaDecoder<'a> {
    pub(crate) fn done(&self) -> bool {
        self.rle.done()
    }
}

impl<'a> From<Cow<'a, [u8]>> for DeltaDecoder<'a> {
    fn from(bytes: Cow<'a, [u8]>) -> Self {
        DeltaDecoder {
            rle: RleDecoder::from(bytes),
            absolute_val: 0,
        }
    }
}

impl<'a> Iterator for DeltaDecoder<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        if let Some(delta) = self.rle.next()? {
            if delta < 0 {
                self.absolute_val -= delta.abs() as u64;
            } else {
                self.absolute_val += delta as u64;
            }
            Some(self.absolute_val)
        } else {
            None
        }
    }
}

impl<'a, 'b> Source for &'a mut DeltaDecoder<'b> {
    fn done(&self) -> bool {
        DeltaDecoder::done(self)
    }
}

impl<'a> Sink for DeltaEncoder<'a> {
    type Item = u64;

    fn append(&mut self, item: Option<Self::Item>) {
        match item {
            Some(i) => self.append_value(i),
            None => self.append_null(),
        }
    }

    fn finish(self) -> usize {
        DeltaEncoder::finish(self)
    }
}

