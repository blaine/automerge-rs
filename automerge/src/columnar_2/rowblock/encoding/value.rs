use crate::columnar_2::rowblock::column_layout::ColumnSpliceError;
use std::{convert::TryInto, ops::Range};

use super::{RawDecoder, RawEncoder, RleDecoder, RleEncoder, Source};
use crate::columnar_2::rowblock::value::PrimVal;

#[derive(Clone)]
pub(crate) struct ValueDecoder<'a> {
    meta: RleDecoder<'a, u64>,
    raw: RawDecoder<'a>,
}

impl<'a> ValueDecoder<'a> {
    pub(crate) fn new(meta: RleDecoder<'a, u64>, raw: RawDecoder<'a>) -> ValueDecoder<'a> {
        ValueDecoder { meta, raw }
    }

    pub(crate) fn done(&self) -> bool {
        self.meta.done()
    }

    pub(crate) fn next(&mut self) -> Option<PrimVal> {
        match self.meta.next() {
            Some(Some(next)) => {
                let val_meta = ValueMeta::from(next);
                #[allow(clippy::redundant_slicing)]
                match val_meta.type_code() {
                    ValueType::Null => Some(PrimVal::Null),
                    ValueType::True => Some(PrimVal::Bool(true)),
                    ValueType::False => Some(PrimVal::Bool(false)),
                    ValueType::Uleb => {
                        let raw = self.raw.read_bytes(val_meta.length()).unwrap();
                        let val = leb128::read::unsigned(&mut &raw[..]).unwrap();
                        Some(PrimVal::Uint(val))
                    }
                    ValueType::Leb => {
                        let raw = self.raw.read_bytes(val_meta.length()).unwrap();
                        let val = leb128::read::signed(&mut &raw[..]).unwrap();
                        Some(PrimVal::Int(val))
                    }
                    ValueType::String => {
                        let raw = self.raw.read_bytes(val_meta.length()).unwrap();
                        let val = String::from_utf8(raw.to_vec()).unwrap();
                        Some(PrimVal::String(val.into()))
                    }
                    ValueType::Float => {
                        assert!(val_meta.length() == 8);
                        let raw: [u8; 8] = self.raw.read_bytes(8).unwrap().try_into().unwrap();
                        let val = f64::from_le_bytes(raw);
                        Some(PrimVal::Float(val))
                    }
                    ValueType::Counter => {
                        let raw = self.raw.read_bytes(val_meta.length()).unwrap();
                        let val = leb128::read::unsigned(&mut &raw[..]).unwrap();
                        Some(PrimVal::Counter(val))
                    }
                    ValueType::Timestamp => {
                        let raw = self.raw.read_bytes(val_meta.length()).unwrap();
                        let val = leb128::read::unsigned(&mut &raw[..]).unwrap();
                        Some(PrimVal::Timestamp(val))
                    }
                    ValueType::Unknown(code) => {
                        let raw = self.raw.read_bytes(val_meta.length()).unwrap();
                        Some(PrimVal::Unknown {
                            type_code: code,
                            data: raw.to_vec(),
                        })
                    }
                    ValueType::Bytes => {
                        let raw = self.raw.read_bytes(val_meta.length()).unwrap();
                        Some(PrimVal::Bytes(raw.to_vec()))
                    }
                }
            }
            Some(None) => Some(PrimVal::Null),
            _ => None,
        }
    }

    pub(crate) fn splice<'b, F: Fn(usize) -> Result<Option<&'b PrimVal>, ColumnSpliceError>>(
        &mut self,
        out: &mut Vec<u8>,
        start: usize,
        replace: Range<usize>,
        replace_with: F,
    ) -> Result<(Range<usize>, Range<usize>), ColumnSpliceError> {
        // Our semantics here are similar to those of Vec::splice. We can describe this
        // imperatively like this:
        //
        // * First copy everything up to the start of `replace` into the output
        // * For every index in `replace` skip that index from ourselves and if `replace_with`
        //   returns `Some` then copy that value to the output
        // * Once we have iterated past `replace.end` we continue to call `replace_with` until it
        //   returns None, copying the results to the output
        // * Finally we copy the remainder of our data into the output
        //
        // However, things are complicated by the fact that our data is stored in two columns. This
        // means that we do this in two passes. First we execute the above logic for the metadata
        // column. Then we do it all over again for the value column.

        // First pass - metadata
        //
        // Copy the metadata decoder so we can iterate over it again when we read the values in the
        // second pass
        let mut meta_copy = self.meta.clone();
        let mut meta_out = RleEncoder::from(&mut *out);
        let mut idx = 0;
        // Copy everything up to replace.start to the output
        while idx < replace.start {
            let val = meta_copy.next().unwrap_or(None);
            meta_out.append(val);
            idx += 1;
        }
        // Now step through replace, skipping our data and inserting the replacement data (if there
        // is any)
        for i in 0..replace.len() {
            meta_copy.next();
            if let Some(val) = replace_with(i)? {
                // Note that we are just constructing metadata values here.
                let meta_val = ValueMeta::from(val).into();
                meta_out.append(Some(meta_val));
            }
            idx += 1;
        }
        // Copy any remaining input from the replacments to the output
        while let Some(val) = replace_with(idx - replace.start)? {
            let meta_val = ValueMeta::from(val).into();
            meta_out.append(Some(meta_val));
            idx += 1;
        }
        // Now copy any remaining data we have to the output
        while !meta_copy.done() {
            let val = meta_copy.next().unwrap_or(None);
            meta_out.append(val);
        }
        let meta_len = meta_out.finish();
        let meta_range = start..meta_len;

        // Second pass, copying the values. For this pass we iterate over ourselves.
        //
        //
        let mut value_range_len = 0;
        let mut raw_encoder = RawEncoder::from(out);
        idx = 0;
        // Copy everything up to replace.start to the output
        while idx < replace.start {
            let val = self.next().unwrap_or(PrimVal::Null);
            value_range_len += encode_primval(&mut raw_encoder, &val);
            idx += 1;
        }

        // Now step through replace, skipping our data and inserting the replacement data (if there
        // is any)
        for i in 0..replace.len() {
            self.next();
            if let Some(val) = replace_with(i)? {
                value_range_len += encode_primval(&mut raw_encoder, &val);
            }
            idx += 1;
        }
        // Copy any remaining input from the replacments to the output
        while let Some(val) = replace_with(idx - replace.start)? {
            value_range_len += encode_primval(&mut raw_encoder, &val);
            idx += 1;
        }
        // Now copy any remaining data we have to the output
        while !self.done() {
            let val = self.next().unwrap_or(PrimVal::Null);
            value_range_len += encode_primval(&mut raw_encoder, &val);
        }

        let value_range = meta_range.end..(meta_range.end + value_range_len);

        Ok((meta_range, value_range))
    }
}

fn encode_primval(out: &mut RawEncoder, val: &PrimVal) -> usize {
    match val {
        PrimVal::Uint(i) => out.append(i),
        PrimVal::Int(i) => out.append(i),
        PrimVal::Null => 0,
        PrimVal::Bool(b) => 0,
        PrimVal::Timestamp(i) => out.append(i),
        PrimVal::Float(f) => out.append(f),
        PrimVal::Counter(i) => out.append(i),
        PrimVal::String(s) => out.append(s),
        PrimVal::Bytes(b) => out.append(b),
        PrimVal::Unknown { data, .. } => out.append(data),
    }
}

impl<'a> Iterator for ValueDecoder<'a> {
    type Item = PrimVal;

    fn next(&mut self) -> Option<Self::Item> {
        ValueDecoder::next(self)
    }
}

impl<'a> Source for ValueDecoder<'a> {
    fn done(&self) -> bool {
        ValueDecoder::done(self)
    }
}

enum ValueType {
    Null,
    False,
    True,
    Uleb,
    Leb,
    Float,
    String,
    Bytes,
    Counter,
    Timestamp,
    Unknown(u8),
}

struct ValueMeta(u64);

impl ValueMeta {
    fn type_code(&self) -> ValueType {
        let low_byte = (self.0 & 0b00001111) as u8;
        match low_byte {
            0 => ValueType::Null,
            1 => ValueType::False,
            2 => ValueType::True,
            3 => ValueType::Uleb,
            4 => ValueType::Leb,
            5 => ValueType::Float,
            6 => ValueType::String,
            7 => ValueType::Bytes,
            8 => ValueType::Counter,
            9 => ValueType::Timestamp,
            other => ValueType::Unknown(other),
        }
    }

    fn length(&self) -> usize {
        (self.0 >> 4) as usize
    }
}

impl From<&PrimVal> for ValueMeta {
    fn from(p: &PrimVal) -> Self {
        match p {
            PrimVal::Uint(i) => Self((ulebsize(*i) << 4) | 3),
            PrimVal::Int(i) => Self((lebsize(*i) << 4) | 4),
            PrimVal::Null => Self(0),
            PrimVal::Bool(b) => Self(match b {
                false => 1,
                true => 2,
            }),
            PrimVal::Timestamp(i) => Self((ulebsize(*i) << 4) | 9),
            PrimVal::Float(_) => Self((8 << 4) | 5),
            PrimVal::Counter(i) => Self((ulebsize(*i) << 4) | 8),
            PrimVal::String(s) => Self(((s.as_bytes().len() as u64) << 4) | 6),
            PrimVal::Bytes(b) => Self(((b.len() as u64) << 4) | 7),
            PrimVal::Unknown { type_code, data } => {
                Self(((data.len() as u64) << 4) | (*type_code as u64))
            }
        }
    }
}

impl From<u64> for ValueMeta {
    fn from(raw: u64) -> Self {
        ValueMeta(raw)
    }
}

impl From<ValueMeta> for u64 {
    fn from(v: ValueMeta) -> Self {
        v.0
    }
}

impl From<&PrimVal> for ValueType {
    fn from(p: &PrimVal) -> Self {
        match p {
            PrimVal::Uint(_) => ValueType::Uleb,
            PrimVal::Int(_) => ValueType::Leb,
            PrimVal::Null => ValueType::Null,
            PrimVal::Bool(b) => match b {
                true => ValueType::True,
                false => ValueType::False,
            },
            PrimVal::Timestamp(_) => ValueType::Timestamp,
            PrimVal::Float(_) => ValueType::Float,
            PrimVal::Counter(_) => ValueType::Counter,
            PrimVal::String(_) => ValueType::String,
            PrimVal::Bytes(_) => ValueType::Bytes,
            PrimVal::Unknown { type_code, .. } => ValueType::Unknown(*type_code),
        }
    }
}

impl From<ValueType> for u64 {
    fn from(v: ValueType) -> Self {
        match v {
            ValueType::Null => 0,
            ValueType::False => 1,
            ValueType::True => 2,
            ValueType::Uleb => 3,
            ValueType::Leb => 4,
            ValueType::Float => 5,
            ValueType::String => 6,
            ValueType::Bytes => 7,
            ValueType::Counter => 8,
            ValueType::Timestamp => 9,
            ValueType::Unknown(other) => other as u64,
        }
    }
}

fn lebsize(val: i64) -> u64 {
    if val == 0 {
        return 1;
    }
    let numbits = (val as f64).abs().log2().ceil() as u64;
    let mut numblocks = (numbits as f64 / 7.0).ceil() as u64;
    // Make room for the sign bit
    if numbits % 7 == 0 {
        numblocks += 1;
    }
    return numblocks;
}

fn ulebsize(val: u64) -> u64 {
    if val == 0 {
        return 1;
    }
    let numbits = (val as f64).log2().ceil() as u64;
    let numblocks = (numbits as f64 / 7.0).ceil() as u64;
    return numblocks;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar_2::rowblock::encoding::{RawDecoder, RleDecoder};
    use proptest::prelude::*;

    fn encode_values(vals: &[PrimVal]) -> (Range<usize>, Range<usize>, Vec<u8>) {
        let mut decoder = ValueDecoder {
            meta: RleDecoder::from(&[] as &[u8]),
            raw: RawDecoder::from(&[] as &[u8]),
        };
        let mut out = Vec::new();
        let (meta_range, val_range) = decoder.splice(&mut out, 0, 0..0, |i| vals.get(i)).unwrap();
        (meta_range, val_range, out)
    }

    fn value() -> impl Strategy<Value = PrimVal> {
        prop_oneof! {
            Just(PrimVal::Null),
            any::<bool>().prop_map(|b| PrimVal::Bool(b)),
            any::<u64>().prop_map(|i| PrimVal::Uint(i)),
            any::<i64>().prop_map(|i| PrimVal::Int(i)),
            any::<f64>().prop_map(|f| PrimVal::Float(f)),
            any::<String>().prop_map(|s| PrimVal::String(s.into())),
            any::<Vec<u8>>().prop_map(|b| PrimVal::Bytes(b)),
            any::<u64>().prop_map(|i| PrimVal::Counter(i)),
            any::<u64>().prop_map(|i| PrimVal::Timestamp(i)),
            (10..15_u8, any::<Vec<u8>>()).prop_map(|(c, b)| PrimVal::Unknown { type_code: c, data: b }),
        }
    }

    #[derive(Clone, Debug)]
    struct Scenario {
        initial_values: Vec<PrimVal>,
        replace_range: Range<usize>,
        replacements: Vec<PrimVal>,
    }

    fn scenario() -> impl Strategy<Value = Scenario> {
        (
            proptest::collection::vec(value(), 0..100),
            proptest::collection::vec(value(), 0..10),
        )
            .prop_flat_map(move |(values, to_splice)| {
                if values.len() == 0 {
                    Just(Scenario {
                        initial_values: values.clone(),
                        replace_range: 0..0,
                        replacements: to_splice.clone(),
                    })
                    .boxed()
                } else {
                    // This is somewhat awkward to write because we have to carry the `values` and
                    // `to_splice` through as `Just(..)` to please the borrow checker.
                    (0..values.len(), Just(values), Just(to_splice))
                        .prop_flat_map(move |(replace_range_start, values, to_splice)| {
                            (
                                0..(values.len() - replace_range_start),
                                Just(values),
                                Just(to_splice),
                            )
                                .prop_map(
                                    move |(replace_range_len, values, to_splice)| Scenario {
                                        initial_values: values.clone(),
                                        replace_range: replace_range_start
                                            ..(replace_range_start + replace_range_len),
                                        replacements: to_splice.clone(),
                                    },
                                )
                        })
                        .boxed()
                }
            })
    }

    proptest! {
        #[test]
        fn test_initialize_splice(values in proptest::collection::vec(value(), 0..100)) {
            let (meta_range, val_range, out) = encode_values(&values);
            let mut decoder = ValueDecoder{
                meta: RleDecoder::from(&out[meta_range]),
                raw: RawDecoder::from(&out[val_range]),
            };
            let mut testvals = Vec::new();
            while !decoder.done() {
                testvals.push(decoder.next().unwrap());
            }
            assert_eq!(values, testvals);
        }

        #[test]
        fn test_splice_values(scenario in scenario()){
            let (meta_range, val_range, out) = encode_values(&scenario.initial_values);
            let mut decoder = ValueDecoder{
                meta: RleDecoder::from(&out[meta_range]),
                raw: RawDecoder::from(&out[val_range]),
            };
            let mut spliced = Vec::new();
            let (spliced_meta, spliced_val) = decoder.splice(&mut spliced, 0, scenario.replace_range.clone(), |i| scenario.replacements.get(i)).unwrap();
            let mut spliced_decoder = ValueDecoder{
                meta: RleDecoder::from(&spliced[spliced_meta]),
                raw: RawDecoder::from(&spliced[spliced_val]),
            };
            let mut result_values = Vec::new();
            while !spliced_decoder.done() {
                result_values.push(spliced_decoder.next().unwrap());
            }
            let mut expected: Vec<_> = scenario.initial_values.clone();
            expected.splice(scenario.replace_range, scenario.replacements);
            assert_eq!(result_values, expected);
        }
    }
}
