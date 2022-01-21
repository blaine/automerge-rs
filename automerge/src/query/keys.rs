use crate::query::{Node, QueryResult, TreeQuery};
use crate::types::Key;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Keys {
    pub keys: Vec<Key>,
}

impl Keys {
    pub fn new() -> Self {
        Keys { keys: vec![] }
    }
}

impl TreeQuery for Keys {
    fn query_node(&mut self, child: &impl Node) -> QueryResult {
        let mut last = None;
        for i in 0..child.len() {
            let op = child.get(i).unwrap();
            if Some(op.key) != last && op.visible() {
                self.keys.push(op.key);
                last = Some(op.key);
            }
        }
        QueryResult::Finish
    }
}
