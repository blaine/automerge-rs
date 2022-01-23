use std::fmt::Debug;

use crate::types::Op;

pub(crate) trait OpBlock:
    IntoIterator<Item = Op> + Extend<Op> + Sized + Clone + Debug
{
    fn build() -> Self;

    fn get(&self, index: usize) -> Option<&Op>;

    fn replace(&mut self, index: usize, new_op: Op) -> Op;

    fn last(&self) -> Option<&Op>;

    fn push(&mut self, op: Op);

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;

    fn insert(&mut self, index: usize, op: Op);

    fn remove(&mut self, index: usize) -> Op;

    fn pop(&mut self) -> Option<Op>;

    fn split_off(&mut self, at: usize) -> Self;
}

impl OpBlock for Vec<Op> {
    fn build() -> Self {
        Vec::new()
    }

    fn replace(&mut self, index: usize, new_op: Op) -> Op {
        let target = self.as_mut_slice().get_mut(index).unwrap();
        std::mem::replace(target, new_op)
    }

    fn get(&self, index: usize) -> Option<&Op> {
        self.as_slice().get(index)
    }

    fn last(&self) -> Option<&Op> {
        self.as_slice().last()
    }

    fn push(&mut self, op: Op) {
        Vec::push(self, op)
    }

    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn is_empty(&self) -> bool {
        Vec::is_empty(self)
    }

    fn insert(&mut self, index: usize, op: Op) {
        Vec::insert(self, index, op)
    }

    fn remove(&mut self, index: usize) -> Op {
        Vec::remove(self, index)
    }

    fn pop(&mut self) -> Option<Op> {
        Vec::pop(self)
    }

    fn split_off(&mut self, at: usize) -> Self {
        Vec::split_off(self, at)
    }
}
