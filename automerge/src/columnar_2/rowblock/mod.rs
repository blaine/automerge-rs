use std::convert::TryInto;

use self::column_layout::DocOpColumns;

use super::{ColumnId, ColumnSpec};

mod column_layout;
pub(crate) use column_layout::{BadColumnLayout, ColumnLayout};
mod column_range;
mod encoding;
use encoding::GenericColDecoder;
mod generic;
mod row_ops;
mod value;
pub(crate) use value::{PrimVal, CellValue};

pub(crate) struct RowBlock<C> {
    columns: C,
    data: Vec<u8>,
}

impl RowBlock<ColumnLayout> {
    pub(crate) fn new<I: Iterator<Item = (ColumnSpec, std::ops::Range<usize>)>>(
        cols: I,
        data: Vec<u8>,
    ) -> Result<RowBlock<ColumnLayout>, BadColumnLayout> {
        let layout = ColumnLayout::parse(data.len(), cols)?;
        Ok(RowBlock {
            columns: layout,
            data,
        })
    }

    pub(crate) fn into_doc_ops(self) -> Result<RowBlock<column_layout::DocOpColumns>, column_layout::ParseDocColumnError> {
        let doc_cols: column_layout::DocOpColumns = self.columns.try_into()?;
        Ok(RowBlock {
            columns: doc_cols,
            data: self.data,
        })
    }
}

impl<'a> IntoIterator for &'a RowBlock<ColumnLayout> {
    type Item = Vec<(usize, Option<CellValue<'a>>)>;
    type IntoIter = RowBlockIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        RowBlockIter {
            decoders: self
                .columns
                .iter()
                .map(|c| (c.id(), c.decoder(&self.data)))
                .collect(),
        }
    }
}

pub(crate) struct RowBlockIter<'a> {
    decoders: Vec<(ColumnId, GenericColDecoder<'a>)>,
}

impl<'a> Iterator for RowBlockIter<'a> {
    type Item = Vec<(usize, Option<CellValue<'a>>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.decoders.iter().all(|(_, d)| d.done()) {
            None
        } else {
            let mut result = Vec::with_capacity(self.decoders.len());
            for (col_index, (_, decoder)) in self.decoders.iter_mut().enumerate() {
                result.push((col_index, decoder.next()));
            }
            Some(result)
        }
    }
}

impl<'a> IntoIterator for &'a RowBlock<DocOpColumns> {
    type Item = row_ops::DocOp<'a>;
    type IntoIter = column_layout::doc_op_columns::DocOpColumnIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.columns.iter(&self.data)
    }
}
