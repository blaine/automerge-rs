use std::ops::{Bound, Range, RangeBounds};

use super::{ColumnLayout, RowBlock, value::CellValue, column_layout::ColumnSpliceError};

#[derive(Debug, thiserror::Error)]
pub(crate) enum SpliceError {
    #[error("invalid value for column {col_index}")]
    InvalidValueForColumn { row_index: usize, col_index: usize },
}

impl RowBlock<ColumnLayout> {

    /// Create a new block, replacing 0 or more of the rows in this block with a new values
    ///
    /// # Arguments
    /// 
    /// * `replace` - The range of indices to remove
    /// * `replace_with` - A closure which takes two arguments (col index, row index) and returns
    ///                    an optional [`CellValue`] which will become the value for the new row.
    ///                    The row index starts at 0 for the first item to be inserted.
    ///
    /// # Errors
    ///
    /// This will return an error if the `CellValue` returned by `replace_with` is not compatible
    /// with the column at that index.
    ///
    /// # Panics
    ///
    /// This function will panic if the indices of `replace` are not in the block
    pub(crate) fn splice<R, F>(
        &self,
        replace: R,
        replace_with: F,
    ) -> Result<Self, SpliceError> 
    where
        R: RangeBounds<usize>,
        F: Fn(usize, usize) -> Option<CellValue>
    {
        let mut new_data = Vec::with_capacity(self.data.len() * 2);
        let mut start = 0;
        let mut output = Vec::new();
        let replace_range = range_bounds_to_range(replace, self.data.len());
        for (index, column) in self.columns.iter().enumerate() {
            let replace_with = |row_index| replace_with(index, row_index);
            let new = column.splice(&self.data, &mut new_data, start, replace_range.clone(), replace_with).map_err(|e| match e {
                ColumnSpliceError::InvalidValueForRow(row) => SpliceError::InvalidValueForColumn{
                    row_index: row,
                    col_index: index,
                }
            })?;
            start += new.range().end;
            output.push(new);
        }
        let result_layout = ColumnLayout::unsafe_from_vec(output);
        new_data.shrink_to_fit();
        Ok(Self{columns: result_layout, data: new_data})
    }
}

fn range_bounds_to_range<R: RangeBounds<usize>>(bounds: R, data_len: usize) -> Range<usize> {
    let start = match bounds.start_bound() {
        Bound::Included(i) => *i,
        Bound::Excluded(i) => *i + 1,
        Bound::Unbounded => 0,
    };
    let end = match bounds.end_bound() {
        Bound::Included(i) => *i,
        Bound::Excluded(i) => *i - 1,
        Bound::Unbounded => data_len - 1,
    };
    start..end
}
