use std::{
    borrow::{Borrow, Cow},
    ops::Range,
};

use crate::columnar_2::rowblock::{column_layout::ColumnSpliceError, value::CellValue};

use super::{
    BooleanDecoder, BooleanEncoder, DeltaDecoder, DeltaEncoder, RleDecoder, RleEncoder,
    Sink, Source, ValueDecoder,
};

pub(crate) enum SimpleColDecoder<'a> {
    RleUint(RleDecoder<'a, u64>),
    RleString(RleDecoder<'a, smol_str::SmolStr>),
    Delta(DeltaDecoder<'a>),
    Bool(BooleanDecoder<'a>),
}

impl<'a> SimpleColDecoder<'a> {
    pub(crate) fn new_uint(d: RleDecoder<'a, u64>) -> Self {
        Self::RleUint(d)
    }

    pub(crate) fn new_string(d: RleDecoder<'a, smol_str::SmolStr>) -> Self {
        Self::RleString(d)
    }

    pub(crate) fn new_delta(d: DeltaDecoder<'a>) -> Self {
        Self::Delta(d)
    }

    pub(crate) fn new_bool(d: BooleanDecoder<'a>) -> Self {
        Self::Bool(d)
    }

    pub(crate) fn done(&self) -> bool {
        match self {
            Self::RleUint(d) => d.done(),
            Self::RleString(d) => d.done(),
            Self::Delta(d) => d.done(),
            Self::Bool(d) => d.done(),
        }
    }

    pub(crate) fn next(&mut self) -> Option<CellValue<'a>> {
        match self {
            Self::RleUint(d) => d.next().and_then(|i| i.map(CellValue::Uint)),
            Self::RleString(d) => d
                .next()
                .and_then(|s| s.map(|s| CellValue::String(Cow::Owned(s.into())))),
            Self::Delta(d) => d.next().map(CellValue::Uint),
            Self::Bool(d) => d.next().map(CellValue::Bool),
        }
    }

    pub(crate) fn splice<'b, F: Fn(usize) -> Option<CellValue<'b>>>(
        &mut self,
        out: &mut Vec<u8>,
        replace: Range<usize>,
        replace_with: F,
    ) -> Result<usize, ColumnSpliceError> {
        match self {
            Self::RleUint(d) => {
                println!("Splicing int column");
                let encoder: RleEncoder<'_, u64> = RleEncoder::from(out);
                do_replace(
                    d.map(|i| i.unwrap_or(0)),
                    encoder,
                    replace,
                    |i| match replace_with(i) {
                        Some(CellValue::Uint(i)) => Ok(Some(i)),
                        Some(_) => Err(ColumnSpliceError::InvalidValueForRow(i)),
                        None => Ok(None),
                    },
                )
            }
            Self::RleString(d) => {
                let encoder = RleEncoder::from(out);
                do_replace(
                    d.map(|i| i.map(Cow::Owned)),
                    encoder,
                    replace,
                    |i| match replace_with(i) {
                        Some(CellValue::String(s)) => Ok(Some(Some(s))),
                        Some(_) => Err(ColumnSpliceError::InvalidValueForRow(i)),
                        None => Ok(None),
                    },
                )
            }
            Self::Delta(d) => {
                let encoder = DeltaEncoder::from(out);
                do_replace(d, encoder, replace, |i| match replace_with(i) {
                    Some(CellValue::Uint(i)) => Ok(Some(i)),
                    Some(_) => Err(ColumnSpliceError::InvalidValueForRow(i)),
                    None => Ok(None),
                })
            }
            Self::Bool(b) => {
                let encoder = BooleanEncoder::from(out);
                do_replace(b, encoder, replace, |i| match replace_with(i) {
                    Some(CellValue::Bool(b)) => Ok(Some(b)),
                    Some(_) => Err(ColumnSpliceError::InvalidValueForRow(i)),
                    None => Ok(None),
                })
            }
        }
    }
}

fn do_replace<I, O, F, V, W>(
    mut input: I,
    mut output: O,
    replace: Range<usize>,
    f: F,
) -> Result<usize, ColumnSpliceError>
where
    W: Borrow<V> + std::fmt::Debug,
    I: Iterator<Item = W>,
    O: Sink<Item = V>,
    F: Fn(usize) -> Result<Option<W>, ColumnSpliceError>,
{
    let mut idx = 0;
    while idx < replace.start {
        let val = input.next();
        output.append(val);
        idx += 1;
    }
    for i in 0..replace.len() {
        let val = f(i)?;
        output.append(val);
        idx += 1;
    }
    while let Some(val) = input.next() {
        output.append(Some(val));
        idx += 1;
    }
    Ok(output.finish())
}

pub(crate) enum SingleLogicalColDecoder<'a> {
    Simple(SimpleColDecoder<'a>),
    Value(ValueDecoder<'a>),
}

impl<'a> Iterator for SingleLogicalColDecoder<'a> {
    type Item = CellValue<'a>;

    fn next(&mut self) -> Option<CellValue<'a>> {
        match self {
            Self::Simple(s) => s.next(),
            Self::Value(v) => v.next().map(|v| CellValue::Value(v)),
        }
    }
}

pub(crate) enum GenericColDecoder<'a> {
    Simple(SimpleColDecoder<'a>),
    Value(ValueDecoder<'a>),
    Group(GroupDecoder<'a>),
}

impl<'a> GenericColDecoder<'a> {
    pub(crate) fn new_simple(s: SimpleColDecoder<'a>) -> Self {
        Self::Simple(s)
    }

    pub(crate) fn new_value(v: ValueDecoder<'a>) -> Self {
        Self::Value(v)
    }

    pub(crate) fn new_group(g: GroupDecoder<'a>) -> Self {
        Self::Group(g)
    }

    pub(crate) fn done(&self) -> bool {
        match self {
            Self::Simple(s) => s.done(),
            Self::Group(g) => g.done(),
            Self::Value(v) => v.done(),
        }
    }

    pub(crate) fn next(&mut self) -> Option<CellValue<'a>> {
        match self {
            Self::Simple(s) => s.next(),
            Self::Value(v) => v.next().map(|v| CellValue::Value(v)),
            Self::Group(g) => g.next().map(|v| CellValue::List(v)),
        }
    }
}

impl<'a> Iterator for GenericColDecoder<'a> {
    type Item = CellValue<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        GenericColDecoder::next(self)
    }
}

impl<'a> Source for GenericColDecoder<'a> {
    fn done(&self) -> bool {
        GenericColDecoder::done(self)
    }
}

pub(crate) struct GroupDecoder<'a> {
    num: RleDecoder<'a, u64>,
    values: Vec<SingleLogicalColDecoder<'a>>,
}

impl<'a> GroupDecoder<'a> {
    pub(crate) fn new(
        num: RleDecoder<'a, u64>,
        values: Vec<SingleLogicalColDecoder<'a>>,
    ) -> GroupDecoder<'a> {
        GroupDecoder { num, values }
    }

    fn next(&mut self) -> Option<Vec<Vec<CellValue<'a>>>> {
        match self.num.next() {
            Some(Some(num_rows)) => {
                let mut result = Vec::with_capacity(num_rows as usize);
                for _ in 0..num_rows {
                    let mut row = Vec::with_capacity(self.values.len());
                    for column in self.values.iter_mut() {
                        row.push(column.next().unwrap());
                    }
                    result.push(row)
                }
                Some(result)
            }
            _ => None,
        }
    }

    fn done(&self) -> bool {
        self.num.done()
    }
}
