use std::ops::Range;

use crate::columnar_2::column_specification::{ColumnId, ColumnSpec, ColumnType};
use super::column::{ColumnBuilder, AwaitingRawColumnValueBuilder, GroupBuilder, GroupAwaitingValue, Column, EmptyGroup};

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

    pub(crate) fn empty() -> Self {
        Self(Vec::new())
    }

    pub(crate) fn append(&mut self, col: Column) {
        self.0.push(col)
    }

    pub(crate) fn unsafe_from_vec(v: Vec<Column>) -> Self {
        Self(v)
    }
}

impl IntoIterator for ColumnLayout {
    type Item = Column;
    type IntoIter = std::vec::IntoIter<Column>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
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

impl From<EmptyGroup> for BadColumnLayout {
    fn from(_: EmptyGroup) -> Self {
        Self::EmptyGroup
    }
}

struct ColumnLayoutParser {
    columns: Vec<Column>,
    last_spec: Option<ColumnSpec>,
    state: LayoutParserState,
    total_data_size: usize,
}

enum LayoutParserState {
    Ready,
    InValue(AwaitingRawColumnValueBuilder),
    InGroup(ColumnId, GroupParseState),
}

#[derive(Debug)]
enum GroupParseState {
    Ready(GroupBuilder),
    InValue(GroupAwaitingValue),
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
            LayoutParserState::InValue(mut builder) => {
                self.columns.push(builder.build(0..0));
                Ok(ColumnLayout(self.columns))
            }
            LayoutParserState::InGroup(_, groupstate) => {
                match groupstate {
                    GroupParseState::InValue(mut builder) => {
                        self.columns.push(builder.finish_empty().finish()?);
                    }
                    GroupParseState::Ready(mut builder) => {
                        self.columns.push(builder.finish()?);
                    }
                };
                Ok(ColumnLayout(self.columns))
            }
        }
    }

    fn add_column(
        &mut self,
        column: ColumnSpec,
        range: Range<usize>,
    ) -> Result<(), BadColumnLayout> {
        self.check_contiguous(&range)?;
        self.check_bounds(&range)?;
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
                        GroupParseState::Ready(ColumnBuilder::start_group(column.id(), range)),
                    );
                    Ok(())
                }
                ColumnType::ValueMetadata => {
                    self.state = LayoutParserState::InValue(ColumnBuilder::start_value(column.id(), range));
                    Ok(())
                }
                ColumnType::Value => Err(BadColumnLayout::LoneRawValueColumn),
                ColumnType::Actor => {
                    self.columns
                        .push(ColumnBuilder::build_actor(column, range));
                    Ok(())
                }
                ColumnType::String => {
                    self.columns
                        .push(ColumnBuilder::build_string(column, range));
                    Ok(())
                }
                ColumnType::Integer => {
                    self.columns
                        .push(ColumnBuilder::build_integer(column, range));
                    Ok(())
                }
                ColumnType::DeltaInteger => {
                    self.columns
                        .push(ColumnBuilder::build_delta_integer(column, range));
                    Ok(())
                }
                ColumnType::Boolean => {
                    self.columns
                        .push(ColumnBuilder::build_boolean(column, range));
                    Ok(())
                }
            },
            LayoutParserState::InValue(builder) => match column.col_type() {
                ColumnType::Value => {
                    if builder.id() != column.id() {
                        return Err(BadColumnLayout::MismatchingValueMetadataId);
                    }
                    self.columns.push(builder.build(range));
                    self.state = LayoutParserState::Ready;
                    Ok(())
                }
                _ => {
                    self.columns.push(builder.build(0..0));
                    self.state = LayoutParserState::Ready;
                    self.add_column(column, range)
                }
            },
            LayoutParserState::InGroup(id, group_state) => {
                if *id != column.id() {
                    match group_state {
                        GroupParseState::Ready(b) => self.columns.push(b.finish()?),
                        GroupParseState::InValue(b) => self.columns.push(b.finish_empty().finish()?),
                    };
                    std::mem::swap(&mut self.state, &mut LayoutParserState::Ready);
                    self.add_column(column, range)
                } else {
                    match group_state {
                        GroupParseState::Ready(builder) => match column.col_type() {
                            ColumnType::Group => Err(BadColumnLayout::NestedGroup),
                            ColumnType::Value => Err(BadColumnLayout::LoneRawValueColumn),
                            ColumnType::ValueMetadata => {
                                *group_state = GroupParseState::InValue(builder.start_value(column, range));
                                Ok(())
                            }
                            ColumnType::Actor => {
                                builder.add_actor(column, range);
                                Ok(())
                            }
                            ColumnType::Boolean => {
                                builder.add_boolean(column, range);
                                Ok(())
                            }
                            ColumnType::DeltaInteger => {
                                builder.add_delta_integer(column, range);
                                Ok(())
                            }
                            ColumnType::Integer => {
                                builder.add_integer(column, range);
                                Ok(())
                            }
                            ColumnType::String => {
                                builder.add_string(column, range);
                                Ok(())
                            }
                        },
                        GroupParseState::InValue(builder) => match column.col_type() {
                            ColumnType::Value => {
                                *group_state = GroupParseState::Ready(builder.finish_value(range));
                                Ok(())
                            }
                            _ => {
                                *group_state = GroupParseState::Ready(builder.finish_empty());
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
            LayoutParserState::InValue(builder) => {
                if builder.meta_range().end != next_range.start {
                    Err(BadColumnLayout::NonContiguousColumns)
                } else {
                    Ok(())
                }
            },
            LayoutParserState::InGroup(_, group_state) => {
                let end = match group_state {
                    GroupParseState::InValue(b) => b.range().end,
                    GroupParseState::Ready(b) => b.range().end,
                };
                if end != next_range.start {
                    println!("Group state: {:?}", group_state);
                    Err(BadColumnLayout::NonContiguousColumns)
                } else {
                    Ok(())
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
