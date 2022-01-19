use std::{
    borrow::Cow,
    convert::{TryFrom, TryInto},
    ops::Range,
};

use super::column::{Column, GroupedColumn, SimpleColType};
use super::value::{CellValue, PrimVal};
use crate::decoding::{BooleanDecoder, Decoder, DeltaDecoder, RleDecoder};

pub(crate) enum SimpleColDecoder<'a> {
    RleUint(RleDecoder<'a, u64>),
    RleString(RleDecoder<'a, String>),
    Value {
        meta: RleDecoder<'a, u64>,
        raw: Decoder<'a>,
    },
    Delta(DeltaDecoder<'a>),
    Bool(BooleanDecoder<'a>),
}

impl<'a> SimpleColDecoder<'a> {
    fn from_type(col_type: SimpleColType, data: &'a [u8]) -> SimpleColDecoder<'a> {
        match col_type {
            SimpleColType::Actor => Self::RleUint(RleDecoder::from(Cow::from(data))),
            SimpleColType::Integer => Self::RleUint(RleDecoder::from(Cow::from(data))),
            SimpleColType::String => Self::RleString(RleDecoder::from(Cow::from(data))),
            SimpleColType::Boolean => Self::Bool(BooleanDecoder::from(Cow::from(data))),
            SimpleColType::DeltaInteger => Self::Delta(DeltaDecoder::from(Cow::from(data))),
        }
    }

    fn done(&self) -> bool {
        match self {
            Self::RleUint(d) => d.done(),
            Self::RleString(d) => d.done(),
            Self::Delta(d) => d.done(),
            Self::Value { meta, .. } => meta.done(),
            Self::Bool(d) => d.done(),
        }
    }

    fn next(&mut self) -> Option<CellValue> {
        match self {
            Self::RleUint(d) => d.next().and_then(|i| i.map(CellValue::Uint)),
            Self::RleString(d) => d.next().and_then(|s| s.map(CellValue::String)),
            Self::Delta(d) => d.next().and_then(|i| i.map(CellValue::Uint)),
            Self::Bool(d) => d.next().map(CellValue::Bool),
            Self::Value { meta, raw } => match meta.next() {
                Some(Some(next)) => {
                    let val_meta = ValueMeta::from(next);
                    #[allow(clippy::redundant_slicing)]
                    match val_meta.type_code() {
                        ValueType::Null => Some(CellValue::Value(PrimVal::Null)),
                        ValueType::True => Some(CellValue::Value(PrimVal::Bool(true))),
                        ValueType::False => Some(CellValue::Value(PrimVal::Bool(false))),
                        ValueType::Uleb => {
                            let raw = raw.read_bytes(val_meta.length()).unwrap();
                            let val = leb128::read::unsigned(&mut &raw[..]).unwrap();
                            Some(CellValue::Value(PrimVal::Uint(val)))
                        }
                        ValueType::Leb => {
                            let raw = raw.read_bytes(val_meta.length()).unwrap();
                            let val = leb128::read::signed(&mut &raw[..]).unwrap();
                            Some(CellValue::Value(PrimVal::Int(val)))
                        }
                        ValueType::String => {
                            let raw = raw.read_bytes(val_meta.length()).unwrap();
                            let val = String::from_utf8(raw.to_vec()).unwrap();
                            Some(CellValue::Value(PrimVal::String(val)))
                        }
                        ValueType::Float => {
                            assert!(val_meta.length() == 8);
                            let raw: [u8; 8] = raw.read_bytes(8).unwrap().try_into().unwrap();
                            let val = f64::from_le_bytes(raw);
                            Some(CellValue::Value(PrimVal::Float(val)))
                        }
                        ValueType::Counter => {
                            let raw = raw.read_bytes(val_meta.length()).unwrap();
                            let val = leb128::read::unsigned(&mut &raw[..]).unwrap();
                            Some(CellValue::Value(PrimVal::Counter(val)))
                        }
                        ValueType::Timestamp => {
                            let raw = raw.read_bytes(val_meta.length()).unwrap();
                            let val = leb128::read::unsigned(&mut &raw[..]).unwrap();
                            Some(CellValue::Value(PrimVal::Timestamp(val)))
                        }
                        ValueType::Unknown(code) => {
                            let raw = raw.read_bytes(val_meta.length()).unwrap();
                            Some(CellValue::Value(PrimVal::Unknown {
                                type_code: code,
                                data: raw.to_vec(),
                            }))
                        }
                        ValueType::Bytes => {
                            let raw = raw.read_bytes(val_meta.length()).unwrap();
                            Some(CellValue::Value(PrimVal::Bytes(raw.to_vec())))
                        }
                    }
                }
                _ => Some(CellValue::List(Vec::new())),
            },
        }
    }
}

pub(crate) enum ColDecoder<'a> {
    Simple(SimpleColDecoder<'a>),
    Group {
        num: RleDecoder<'a, u64>,
        values: Vec<SimpleColDecoder<'a>>,
    },
}

impl<'a> ColDecoder<'a> {
    pub(crate) fn from_col(col: &'a Column, data: &'a [u8]) -> ColDecoder<'a> {
        match col {
            Column::Single(_, col_type, range) => {
                let data = &data[Range::from(range)];
                Self::Simple(SimpleColDecoder::from_type(*col_type, data))
            }
            Column::Value { meta, value, .. } => Self::Simple(SimpleColDecoder::Value {
                meta: RleDecoder::from(Cow::from(&data[Range::from(meta)])),
                raw: Decoder::from(Cow::from(&data[Range::from(value)])),
            }),
            Column::Group { num, values, .. } => {
                let num_coder = RleDecoder::from(Cow::from(&data[Range::from(num)]));
                let values = values
                    .iter()
                    .map(|gc| match gc {
                        GroupedColumn::Single(_, col_type, d) => {
                            SimpleColDecoder::from_type(*col_type, &data[Range::from(d)])
                        }
                        GroupedColumn::Value { meta, value } => SimpleColDecoder::Value {
                            meta: RleDecoder::from(Cow::from(&data[Range::from(meta)])),
                            raw: Decoder::from(Cow::from(&data[Range::from(value)])),
                        },
                    })
                    .collect();
                Self::Group {
                    num: num_coder,
                    values,
                }
            }
        }
    }

    pub(crate) fn done(&self) -> bool {
        match self {
            Self::Simple(s) => s.done(),
            Self::Group { num, .. } => num.done(),
        }
    }

    pub(crate) fn next(&mut self) -> Option<CellValue> {
        match self {
            Self::Simple(s) => s.next(),
            Self::Group { num, values } => match num.next() {
                Some(Some(num_rows)) => {
                    let mut result = Vec::with_capacity(num_rows as usize);
                    for _ in 0..num_rows {
                        let mut row = Vec::with_capacity(values.len());
                        for column in values.iter_mut() {
                            row.push(column.next().unwrap());
                        }
                        result.push(row)
                    }
                    Some(CellValue::List(result))
                }
                _ => Some(CellValue::List(Vec::new())),
            },
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
