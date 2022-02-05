use std::{
    borrow::Cow,
    ops::{Range, RangeBounds, Bounds},
};

use crate::columnar_2::rowblock::{
    column_layout::{ColumnSpliceError, column::{Column, GroupedColumn, SimpleColType}},
    value::CellValue,
};

use super::{BooleanDecoder, DeltaDecoder, RawDecoder, RleDecoder, Source, ValueDecoder, RleEncoder};

pub(crate) enum SimpleColDecoder<'a> {
    RleUint(RleDecoder<'a, u64>),
    RleString(RleDecoder<'a, String>),
    Value(ValueDecoder<'a>),
    Delta(DeltaDecoder<'a>),
    Bool(BooleanDecoder<'a>),
}

impl<'a> SimpleColDecoder<'a> {
    pub(crate) fn from_type(col_type: SimpleColType, data: &'a [u8]) -> SimpleColDecoder<'a> {
        match col_type {
            SimpleColType::Actor => Self::RleUint(RleDecoder::from(Cow::from(data))),
            SimpleColType::Integer => Self::RleUint(RleDecoder::from(Cow::from(data))),
            SimpleColType::String => Self::RleString(RleDecoder::from(Cow::from(data))),
            SimpleColType::Boolean => Self::Bool(BooleanDecoder::from(Cow::from(data))),
            SimpleColType::DeltaInteger => Self::Delta(DeltaDecoder::from(Cow::from(data))),
        }
    }

    pub(crate) fn done(&self) -> bool {
        match self {
            Self::RleUint(d) => d.done(),
            Self::RleString(d) => d.done(),
            Self::Delta(d) => d.done(),
            Self::Value(value) => value.done(),
            Self::Bool(d) => d.done(),
        }
    }

    pub(crate) fn next(&mut self) -> Option<CellValue> {
        match self {
            Self::RleUint(d) => d.next().and_then(|i| i.map(CellValue::Uint)),
            Self::RleString(d) => d.next().and_then(|s| s.map(CellValue::String)),
            Self::Delta(d) => d.next().and_then(|i| i.map(CellValue::Uint)),
            Self::Bool(d) => d.next().map(CellValue::Bool),
            Self::Value(value) => value.next().map(CellValue::Value),
        }
    }

    pub(crate) fn splice<F: Fn(usize) -> Option<CellValue>>(
        &mut self,
        out: &mut Vec<u8>,
        replace: Range<usize>,
        replace_with: F,
    ) -> Result<usize, ColumnSpliceError> {
        match self {
            Self::RleUint(d) => {
                let encoder = RleEncoder::from(out);
                let mut idx = 0;
                while idx < replace.start {
                    let val = d.next().unwrap_or(None);
                    encoder.append(val);
                    idx += 1;
                }
                for i in 0..replace.len() {
                    let val = match replace_with(i) {
                        Some(CellValue::Uint(i)) => Some(i),
                        Some(_) => return Err(ColumnSpliceError::InvalidValueForRow(i)),
                        None => None,
                    };
                    encoder.append(val);
                }
                while !d.done() {
                    let val = d.next().unwrap_or(None);
                    encoder.append(val);
                    idx += 1;
                }
                Ok(encoder.finish())
            },
            _ => unimplemented!(),
        }
    }
}

pub(crate) enum GenericColDecoder<'a> {
    Simple(SimpleColDecoder<'a>),
    Group {
        num: RleDecoder<'a, u64>,
        values: Vec<SimpleColDecoder<'a>>,
    },
}

impl<'a> GenericColDecoder<'a> {
    pub(crate) fn from_col(col: &'a Column, data: &'a [u8]) -> GenericColDecoder<'a> {
        match col {
            Column::Single(_, col_type, range) => {
                let data = &data[Range::from(range)];
                Self::Simple(SimpleColDecoder::from_type(*col_type, data))
            }
            Column::Value { meta, value, .. } => {
                Self::Simple(SimpleColDecoder::Value(ValueDecoder::new(
                    RleDecoder::from(Cow::Borrowed(&data[Range::from(meta)])),
                    RawDecoder::from(Cow::Borrowed(&data[Range::from(value)])),
                )))
            }
            Column::Group { num, values, .. } => {
                let num_coder = RleDecoder::from(Cow::from(&data[Range::from(num)]));
                let values = values
                    .iter()
                    .map(|gc| match gc {
                        GroupedColumn::Single(_, col_type, d) => {
                            SimpleColDecoder::from_type(*col_type, &data[Range::from(d)])
                        }
                        GroupedColumn::Value { meta, value } => {
                            SimpleColDecoder::Value(ValueDecoder::new(
                                RleDecoder::from(Cow::Borrowed(&data[Range::from(meta)])),
                                RawDecoder::from(Cow::Borrowed(&data[Range::from(value)])),
                            ))
                        }
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

impl<'a> Iterator for GenericColDecoder<'a> {
    type Item = CellValue;

    fn next(&mut self) -> Option<Self::Item> {
        GenericColDecoder::next(self)
    }
}

impl<'a> Source for GenericColDecoder<'a> {
    fn done(&self) -> bool {
        GenericColDecoder::done(self)
    }
}
