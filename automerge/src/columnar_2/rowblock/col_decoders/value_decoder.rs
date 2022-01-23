use std::{borrow::Cow, convert::TryInto};

use crate::{columnar_2::rowblock::value::{CellValue, PrimVal}, decoding::{RleDecoder, Decoder}};


pub(crate) struct ValueDecoder<'a> {
    meta: RleDecoder<'a, u64>,
    raw: Decoder<'a>,
}

impl<'a> ValueDecoder<'a> {
    pub(crate) fn new(meta: &'a[u8], raw: &'a [u8]) -> ValueDecoder<'a> {
        ValueDecoder {
            meta: RleDecoder::from(Cow::from(meta)),
            raw: Decoder::from(Cow::from(raw)),
        }
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
            _ => None,
        }
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

impl From<u64> for ValueMeta {
    fn from(raw: u64) -> Self {
        ValueMeta(raw)
    }
}
