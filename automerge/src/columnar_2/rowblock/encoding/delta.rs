use std::{borrow::{Cow, Borrow}, ops::Range};

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
            .append_value(&(value as i64 - self.absolute_value as i64));
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

    pub(crate) fn splice<I: Iterator<Item=Option<u64>>>(&mut self, replace: Range<usize>, mut replace_with: I, out: &mut Vec<u8>) -> usize {
        let mut encoder = DeltaEncoder::new(out);
        let mut idx = 0;
        while idx < replace.start {
            match self.next() {
                Some(elem) => encoder.append(Some(elem)),
                None => panic!("out of bounds"),
            }
            idx += 1;
        }
        for _ in 0..replace.len() {
            self.next();
            if let Some(next) = replace_with.next() {
                encoder.append(Some(next));
            }
        }
        while let Some(next) = replace_with.next() {
            encoder.append(Some(next));
        }
        while let Some(next) = self.next() {
            encoder.append(Some(next));
        }
        encoder.finish()
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

impl<'a> From<&'a [u8]> for DeltaDecoder<'a> {
    fn from(d: &'a [u8]) -> Self {
        Cow::Borrowed(d).into() 
    }
}

impl<'a> Iterator for DeltaDecoder<'a> {
    type Item = Option<u64>;

    fn next(&mut self) -> Option<Option<u64>> {
        match self.rle.next() {
            Some(Some(delta)) => {
                if delta < 0 {
                    // TODO: This should be fallible and error if this would take the absolute value
                    // below zero
                    self.absolute_val -= delta.abs() as u64;
                } else {
                    self.absolute_val += delta as u64;
                }
                Some(Some(self.absolute_val))
            },
            Some(None) => Some(None),
            None => None,
        }
    }
}

impl<'a, 'b> Source for &'a mut DeltaDecoder<'b> {
    fn done(&self) -> bool {
        DeltaDecoder::done(self)
    }
}

impl<'a> Sink for DeltaEncoder<'a> {
    type Item = Option<u64>;

    fn append<I: Borrow<Self::Item>>(&mut self, item: Option<I>) {
        match item.and_then(|i| i.borrow().clone()) {
            Some(i) => self.append_value(i),
            None => self.append_null(),
        }
    }

    fn finish(self) -> usize {
        DeltaEncoder::finish(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use crate::columnar_2::rowblock::encoding::properties::splice_scenario;

    fn encode(vals: &[Option<u64>]) -> Vec<u8> {
        let mut buf = Vec::<u8>::new();
        let mut encoder = DeltaEncoder::from(&mut buf);
        for val in vals {
            encoder.append(Some(val));
        }
        encoder.finish();
        buf
    }

    fn decode(buf: &[u8]) -> Vec<Option<u64>> {
        DeltaDecoder::from(buf).collect()
    }

    /// DeltaEncoder internally encodes deltas as run length encoded i64s. This means we cannot
    /// represent any number larger than i64::MAX
    fn encodable_u64() -> impl Strategy<Value = Option<u64>> + Clone {
        proptest::option::of(0..(i64::MAX as u64))
    }

    proptest!{
        #[test]
        fn encode_decode_delta(vals in proptest::collection::vec(encodable_u64(), 0..100)) {
            assert_eq!(vals, decode(&encode(&vals)));
        }

        #[test]
        fn splice_delta(scenario in splice_scenario(encodable_u64())) {
            let encoded = encode(&scenario.initial_values);
            let mut decoder = DeltaDecoder::from(&encoded[..]);
            let mut out = Vec::new();
            let len = decoder.splice(scenario.replace_range.clone(), scenario.replacements.iter().cloned(), &mut out);
            let decoded = decode(&out[..]);
            scenario.check(decoded);
            assert_eq!(len, out.len());
        }
    }
}
