use std::ops::{Range, RangeBounds};

use super::{
    super::{encoding::{RleDecoder, RawDecoder, ValueDecoder, SimpleColDecoder}, CellValue, ColumnId, ColumnSpec},
    ColumnSpliceError,
};

#[derive(Clone)]
pub(crate) enum Column {
    Single(ColumnSpec, SimpleColType, CopyRange<usize>),
    Value {
        id: ColumnId,
        meta: CopyRange<usize>,
        value: CopyRange<usize>,
    },
    Group {
        id: ColumnId,
        num: CopyRange<usize>,
        values: Vec<GroupedColumn>,
    },
}

impl Column {
    pub(crate) fn range(&self) -> Range<usize> {
        match self {
            Self::Single(_, _, r) => r.into(),
            Self::Value { meta, value, .. } => (meta.start..value.end),
            Self::Group { num, values, .. } => (num.start..values.last().unwrap().range().end),
        }
    }

    pub(crate) fn splice<'a, F>(
        &self,
        source: &[u8],
        output: &mut Vec<u8>,
        output_start: usize,
        replace: Range<usize>,
        replace_with: F,
    ) -> Result<Self, ColumnSpliceError>
    where
        F: Fn(usize) -> Option<&'a CellValue>,
    {
        match self {
            Self::Single(s, t, range) => {
                let mut decoder = SimpleColDecoder::from_type(*t, &source[Range::from(range)]);
                let end = decoder.splice(output, replace, replace_with)? + output_start;
                Ok(Self::Single(*s, *t, (output_start..end).into()))
            }
            Self::Value { id, meta, value,} => {
                let mut decoder = ValueDecoder{
                    meta: RleDecoder::from(&source[Range::from(meta)]),
                    raw: RawDecoder::from(&source[Range::from(value)]),
                };
                let replacements = |i| {
                    match replace_with(i) {
                        Some(CellValue::Value(p)) => Ok(Some(p)),
                        None => Ok(None),
                        Some(_) => Err(ColumnSpliceError::InvalidValueForRow(i)),
                    }
                };
                let (new_meta, new_data) = decoder.splice(output, output_start, replace, replacements)?;
                Ok(Self::Value{ id: *id, meta: new_meta.into(), value: new_data.into() })
            },
            Self::Group { .. } => unimplemented!(),
        }
    }
}

#[derive(Eq, PartialEq, Clone, Copy)]
pub(crate) enum SimpleColType {
    Actor,
    Integer,
    DeltaInteger,
    Boolean,
    String,
}

#[derive(Clone, Copy)]
pub(crate) enum GroupedColumn {
    Single(ColumnId, SimpleColType, CopyRange<usize>),
    Value {
        meta: CopyRange<usize>,
        value: CopyRange<usize>,
    },
}

impl GroupedColumn {
    pub(crate) fn range(&self) -> Range<usize> {
        match self {
            Self::Single(_, _, r) => r.into(),
            Self::Value { meta, value } => (meta.start..value.end),
        }
    }
}

impl Column {
    pub fn id(&self) -> ColumnId {
        match self {
            Self::Single(s, _, _) => s.id(),
            Self::Value { id, .. } => *id,
            Self::Group { id, .. } => *id,
        }
    }
}

/// std::ops::Range doesn't Copy, so this is a copy of Range which does
#[derive(Clone, Copy)]
pub(crate) struct CopyRange<T> {
    start: T,
    end: T,
}

impl<T> From<Range<T>> for CopyRange<T> {
    fn from(r: Range<T>) -> Self {
        CopyRange {
            start: r.start,
            end: r.end,
        }
    }
}

impl<T> From<CopyRange<T>> for Range<T> {
    fn from(r: CopyRange<T>) -> Self {
        r.start..r.end
    }
}

impl<T> From<&CopyRange<T>> for Range<T>
where
    T: Copy,
{
    fn from(r: &CopyRange<T>) -> Self {
        r.start..r.end
    }
}

impl<T> From<&mut Range<T>> for CopyRange<T>
where
    T: Copy,
{
    fn from(r: &mut Range<T>) -> Self {
        CopyRange {
            start: r.start,
            end: r.end,
        }
    }
}
