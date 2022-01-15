use std::ops::Range;

use super::{
    column_specification::ColumnSpec,
    parse
};

#[derive(Debug)]
pub(super) struct Column {
    spec: ColumnSpec,
    data: Range<usize>,
}

#[derive(Debug)]
pub(super) struct ColumnMetadata(Vec<Column>);

impl ColumnMetadata {
    pub(super) fn parse(input: &[u8]) -> parse::ParseResult<ColumnMetadata> {
        let i = input;
        let (i, num_columns) = parse::leb128_u64(i)?;
        let (i, specs_and_lens) = parse::apply_n(
            num_columns as usize,
            parse::tuple2(
                parse::map(parse::leb128_u32, ColumnSpec::from),
                parse::leb128_u64,
            ),
        )(i)?;
        let columns = specs_and_lens
            .into_iter()
            .scan(0_usize, |offset, (spec, len)| {
                let end = *offset + len as usize;
                let data = *offset..end;
                *offset = end;
                Some(Column { spec, data })
            })
            .collect();
        Ok((i, ColumnMetadata(columns)))
    }

    pub(super) fn total_column_len(&self) -> usize {
        self.0.iter().map(|c| c.data.len()).sum()
    }
}

