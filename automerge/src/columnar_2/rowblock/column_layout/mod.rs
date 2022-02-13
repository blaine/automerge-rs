pub(crate) mod column;
pub(crate) mod generic;
pub(crate) mod doc_op_columns;
pub(crate) mod op_tree_columns;

pub(crate) use generic::{BadColumnLayout, ColumnLayout};
pub(crate) use doc_op_columns::{DocOpColumns, Error as ParseDocColumnError};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ColumnSpliceError {
    #[error("invalid value for row {0}")]
    InvalidValueForRow(usize),
    #[error("wrong number of values for row {0}, expected {expected} but got {actual}")]
    WrongNumberOfValues {
        row: usize,
        expected: usize,
        actual: usize,
    }
}

