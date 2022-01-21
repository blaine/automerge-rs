use crate::query::{QueryResult, TreeQuery, VisWindow};
use crate::types::{Clock, Key, Op};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KeysAt {
    clock: Clock,
    pub keys: Vec<Key>,
    last: Option<Key>,
    window: VisWindow,
    pos: usize,
}

impl KeysAt {
    pub fn new(clock: Clock) -> Self {
        KeysAt {
            clock,
            pos: 0,
            last: None,
            keys: vec![],
            window: Default::default(),
        }
    }
}

impl TreeQuery for KeysAt {
    fn query_element(&mut self, op: &Op) -> QueryResult {
        let visible = self.window.visible_at(op, self.pos, &self.clock);
        if Some(op.key) != self.last && visible {
            self.keys.push(op.key);
            self.last = Some(op.key);
        }
        self.pos += 1;
        QueryResult::Next
    }
}
