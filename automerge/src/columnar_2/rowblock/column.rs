use std::ops::Range;

use super::super::{ColumnId, ColumnSpec};

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
