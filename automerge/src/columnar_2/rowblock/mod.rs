use super::{ColumnId, ColumnSpec};

mod col_decoders;
mod column;
mod column_layout;
use column_layout::{BadColumnLayout, ColumnLayout};
mod op_columns;
mod value;
use value::CellValue;

pub(crate) struct RowBlock {
    columns: ColumnLayout,
    data: Vec<u8>,
}

impl RowBlock {
    pub(crate) fn new<I: Iterator<Item = (ColumnSpec, std::ops::Range<usize>)>>(
        cols: I,
        data: Vec<u8>,
    ) -> Result<RowBlock, BadColumnLayout> {
        let layout = ColumnLayout::parse(data.len(), cols)?;
        Ok(RowBlock {
            columns: layout,
            data,
        })
    }
}

impl<'a> IntoIterator for &'a RowBlock {
    type Item = Vec<(ColumnId, Option<CellValue>)>;
    type IntoIter = RowBlockIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        RowBlockIter {
            decoders: self
                .columns
                .iter()
                .map(|c| (c.id(), col_decoders::ColDecoder::from_col(c, &self.data)))
                .collect(),
        }
    }
}

pub(crate) struct RowBlockIter<'a> {
    decoders: Vec<(ColumnId, col_decoders::ColDecoder<'a>)>,
}

impl<'a> Iterator for RowBlockIter<'a> {
    type Item = Vec<(ColumnId, Option<CellValue>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.decoders.iter().all(|(_, d)| d.done()) {
            None
        } else {
            let mut result = Vec::with_capacity(self.decoders.len());
            for (col_id, decoder) in &mut self.decoders {
                result.push((*col_id, decoder.next()));
            }
            Some(result)
        }
    }
}

