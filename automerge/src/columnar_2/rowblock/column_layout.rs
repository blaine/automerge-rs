use std::ops::Range;

use super::super::column_specification::{ColumnId, ColumnSpec, ColumnType};
use super::column::{Column, GroupedColumn, SimpleColType};

pub(crate) struct ColumnLayout(Vec<Column>);

impl ColumnLayout {
    pub(crate) fn iter(&self) -> impl Iterator<Item = &Column> {
        self.0.iter()
    }

    pub(crate) fn parse<I: Iterator<Item = (ColumnSpec, Range<usize>)>>(
        data_size: usize,
        cols: I,
    ) -> Result<ColumnLayout, BadColumnLayout> {
        let mut parser = ColumnLayoutParser::new(data_size, None);
        for (col, range) in cols {
            parser.add_column(col, range)?;
        }
        parser.build()
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BadColumnLayout {
    #[error("duplicate column specifications: {0}")]
    DuplicateColumnSpecs(u32),
    #[error("out of order columns")]
    OutOfOrder,
    #[error("nested group")]
    NestedGroup,
    #[error("raw value column without metadata column")]
    LoneRawValueColumn,
    #[error("value metadata followed by value column with different column ID")]
    MismatchingValueMetadataId,
    #[error("group column had no following data columns")]
    EmptyGroup,
    #[error("non contiguous columns")]
    NonContiguousColumns,
    #[error("data out of range")]
    DataOutOfRange,
}

struct ColumnLayoutParser {
    columns: Vec<Column>,
    last_spec: Option<ColumnSpec>,
    state: LayoutParserState,
    total_data_size: usize,
}

enum LayoutParserState {
    Ready,
    InValue(ColumnId, Range<usize>),
    InGroup(ColumnId, Range<usize>, Vec<GroupedColumn>, GroupParseState),
}

enum GroupParseState {
    Ready,
    InValue(Range<usize>),
}

impl ColumnLayoutParser {
    fn new(data_size: usize, size_hint: Option<usize>) -> Self {
        ColumnLayoutParser {
            columns: Vec::with_capacity(size_hint.unwrap_or(0)),
            last_spec: None,
            state: LayoutParserState::Ready,
            total_data_size: data_size,
        }
    }

    fn build(mut self) -> Result<ColumnLayout, BadColumnLayout> {
        match self.state {
            LayoutParserState::Ready => Ok(ColumnLayout(self.columns)),
            LayoutParserState::InValue(id, meta_range) => {
                self.columns.push(Column::Value {
                    id,
                    meta: meta_range.into(),
                    value: (0..0).into(),
                });
                Ok(ColumnLayout(self.columns))
            }
            LayoutParserState::InGroup(id, range, mut grouped, groupstate) => {
                if grouped.is_empty() {
                    Err(BadColumnLayout::EmptyGroup)
                } else {
                    match groupstate {
                        GroupParseState::InValue(meta) => {
                            grouped.push(GroupedColumn::Value {
                                meta: meta.into(),
                                value: (0..0).into(),
                            });
                        }
                        GroupParseState::Ready => {
                            self.columns.push(Column::Group {
                                id,
                                num: range.into(),
                                values: grouped,
                            });
                        }
                    };
                    Ok(ColumnLayout(self.columns))
                }
            }
        }
    }

    fn add_column(
        &mut self,
        column: ColumnSpec,
        range: Range<usize>,
    ) -> Result<(), BadColumnLayout> {
        self.check_contiguous(&range)?;
        if let Some(last_spec) = self.last_spec {
            if last_spec.normalize() > column.normalize() {
                return Err(BadColumnLayout::OutOfOrder);
            } else if last_spec == column {
                return Err(BadColumnLayout::DuplicateColumnSpecs(column.into()));
            }
        }
        match &mut self.state {
            LayoutParserState::Ready => match column.col_type() {
                ColumnType::Group => {
                    self.state = LayoutParserState::InGroup(
                        column.id(),
                        range,
                        Vec::new(),
                        GroupParseState::Ready,
                    );
                    Ok(())
                }
                ColumnType::ValueMetadata => {
                    self.state = LayoutParserState::InValue(column.id(), range);
                    Ok(())
                }
                ColumnType::Value => Err(BadColumnLayout::LoneRawValueColumn),
                ColumnType::Actor => {
                    self.columns
                        .push(Column::Single(column, SimpleColType::Actor, range.into()));
                    Ok(())
                }
                ColumnType::String => {
                    self.columns
                        .push(Column::Single(column, SimpleColType::String, range.into()));
                    Ok(())
                }
                ColumnType::Integer => {
                    self.columns
                        .push(Column::Single(column, SimpleColType::Integer, range.into()));
                    Ok(())
                }
                ColumnType::DeltaInteger => {
                    self.columns.push(Column::Single(
                        column,
                        SimpleColType::DeltaInteger,
                        range.into(),
                    ));
                    Ok(())
                }
                ColumnType::Boolean => {
                    self.columns
                        .push(Column::Single(column, SimpleColType::Boolean, range.into()));
                    Ok(())
                }
            },
            LayoutParserState::InValue(id, meta_range) => match column.col_type() {
                ColumnType::Value => {
                    if *id != column.id() {
                        return Err(BadColumnLayout::MismatchingValueMetadataId);
                    }
                    self.columns.push(Column::Value {
                        id: *id,
                        value: range.into(),
                        meta: meta_range.into(),
                    });
                    self.state = LayoutParserState::Ready;
                    Ok(())
                }
                _ => {
                    self.columns.push(Column::Value {
                        id: *id,
                        value: (0..0).into(),
                        meta: meta_range.into(),
                    });
                    self.state = LayoutParserState::Ready;
                    self.add_column(column, range)
                }
            },
            LayoutParserState::InGroup(id, num_range, grouped_cols, group_state) => {
                if *id != column.id() {
                    if grouped_cols.is_empty() {
                        Err(BadColumnLayout::EmptyGroup)
                    } else {
                        let grouped_cols = std::mem::take(grouped_cols);
                        self.columns.push(Column::Group {
                            id: *id,
                            num: num_range.into(),
                            values: grouped_cols,
                        });
                        std::mem::swap(&mut self.state, &mut LayoutParserState::Ready);
                        self.add_column(column, range)
                    }
                } else {
                    match group_state {
                        GroupParseState::Ready => match column.col_type() {
                            ColumnType::Group => Err(BadColumnLayout::NestedGroup),
                            ColumnType::Value => Err(BadColumnLayout::LoneRawValueColumn),
                            ColumnType::ValueMetadata => {
                                *group_state = GroupParseState::InValue(range);
                                Ok(())
                            }
                            ColumnType::Actor => {
                                grouped_cols.push(GroupedColumn::Single(
                                    column.id(),
                                    SimpleColType::Actor,
                                    range.into(),
                                ));
                                Ok(())
                            }
                            ColumnType::Boolean => {
                                grouped_cols.push(GroupedColumn::Single(
                                    column.id(),
                                    SimpleColType::Boolean,
                                    range.into(),
                                ));
                                Ok(())
                            }
                            ColumnType::DeltaInteger => {
                                grouped_cols.push(GroupedColumn::Single(
                                    column.id(),
                                    SimpleColType::DeltaInteger,
                                    range.into(),
                                ));
                                Ok(())
                            }
                            ColumnType::Integer => {
                                grouped_cols.push(GroupedColumn::Single(
                                    column.id(),
                                    SimpleColType::Integer,
                                    range.into(),
                                ));
                                Ok(())
                            }
                            ColumnType::String => {
                                grouped_cols.push(GroupedColumn::Single(
                                    column.id(),
                                    SimpleColType::String,
                                    range.into(),
                                ));
                                Ok(())
                            }
                        },
                        GroupParseState::InValue(meta_range) => match column.col_type() {
                            ColumnType::Value => {
                                grouped_cols.push(GroupedColumn::Value {
                                    meta: meta_range.into(),
                                    value: range.into(),
                                });
                                *group_state = GroupParseState::Ready;
                                Ok(())
                            }
                            _ => {
                                grouped_cols.push(GroupedColumn::Value {
                                    meta: meta_range.into(),
                                    value: (0..0).into(),
                                });
                                *group_state = GroupParseState::Ready;
                                self.add_column(column, range)
                            }
                        },
                    }
                }
            }
        }
    }

    fn check_contiguous(&self, next_range: &Range<usize>) -> Result<(), BadColumnLayout> {
        match &self.state {
            LayoutParserState::Ready => if let Some(prev) = self.columns.last() {
                if prev.range().end != next_range.start {
                    Err(BadColumnLayout::NonContiguousColumns)
                } else {
                    Ok(())
                }
            } else {
                Ok(())
            },
            LayoutParserState::InValue(_, r) => {
                if r.end != next_range.start {
                    Err(BadColumnLayout::NonContiguousColumns)
                } else {
                    Ok(())
                }
            },
            LayoutParserState::InGroup(_, r, cols, group_state) => {
                match group_state {
                    GroupParseState::InValue(r) => if r.end != next_range.start {
                        Err(BadColumnLayout::NonContiguousColumns)
                    } else {
                        Ok(())
                    },
                    GroupParseState::Ready => {
                        match cols.last() {
                            Some(c) => if c.range().end != next_range.start {
                                Err(BadColumnLayout::NonContiguousColumns)
                            } else {
                                Ok(())
                            },
                            None => if r.end != next_range.start {
                                return Err(BadColumnLayout::NonContiguousColumns)
                            } else {
                                Ok(())
                            }
                        }
                    }
                }
            }
        }
    }

    fn check_bounds(&self, next_range: &Range<usize>) -> Result<(), BadColumnLayout> {
        if next_range.end > self.total_data_size {
            Err(BadColumnLayout::DataOutOfRange)
        } else {
            Ok(())
        }
    }
}
