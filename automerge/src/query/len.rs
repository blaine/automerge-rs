use crate::query::{Node, QueryResult, TreeQuery};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Len {
    pub len: usize,
}

impl Len {
    pub fn new() -> Self {
        Len { len: 0 }
    }
}

impl TreeQuery for Len {
    fn query_node(&mut self, child: &impl Node) -> QueryResult {
        self.len = child.visible_len();
        QueryResult::Finish
    }
}
