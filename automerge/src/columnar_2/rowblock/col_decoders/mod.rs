use std::{
    borrow::Cow,
    ops::Range,
};

use super::column_layout::column::{Column, GroupedColumn, SimpleColType};
use super::value::CellValue;
use crate::decoding::{BooleanDecoder, DeltaDecoder, RleDecoder};

mod key_decoder;
pub(crate) use key_decoder::KeyDecoder;
mod obj_decoder;
pub(crate) use obj_decoder::ObjDecoder;
mod op_list_decoder;
pub(crate) use op_list_decoder::OpListDecoder;
mod opid_decoder;
pub(crate) use opid_decoder::OpIdDecoder;
mod value_decoder;
pub(crate) use value_decoder::ValueDecoder;

pub(crate) enum SimpleColDecoder<'a> {
    RleUint(RleDecoder<'a, u64>),
    RleString(RleDecoder<'a, String>),
    Value(ValueDecoder<'a>),
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
            Self::Value(value) => value.done(),
            Self::Bool(d) => d.done(),
        }
    }

    fn next(&mut self) -> Option<CellValue> {
        match self {
            Self::RleUint(d) => d.next().and_then(|i| i.map(CellValue::Uint)),
            Self::RleString(d) => d.next().and_then(|s| s.map(CellValue::String)),
            Self::Delta(d) => d.next().and_then(|i| i.map(CellValue::Uint)),
            Self::Bool(d) => d.next().map(CellValue::Bool),
            Self::Value(value) => value.next(),
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
            Column::Value { meta, value, .. } => Self::Simple(SimpleColDecoder::Value(ValueDecoder::new(
                &data[Range::from(meta)],
                &data[Range::from(value)],
            ))),
            Column::Group { num, values, .. } => {
                let num_coder = RleDecoder::from(Cow::from(&data[Range::from(num)]));
                let values = values
                    .iter()
                    .map(|gc| match gc {
                        GroupedColumn::Single(_, col_type, d) => {
                            SimpleColDecoder::from_type(*col_type, &data[Range::from(d)])
                        }
                        GroupedColumn::Value { meta, value } => SimpleColDecoder::Value(ValueDecoder::new(
                            &data[Range::from(meta)],
                            &data[Range::from(value)],
                        )),
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
