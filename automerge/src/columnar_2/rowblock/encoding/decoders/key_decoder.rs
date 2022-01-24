use  smol_str::SmolStr;

use crate::{
    types::{ElemId, OpId},
    columnar_2::rowblock::row_ops::Key,
    decoding::{RleDecoder, DeltaDecoder},
};


pub(crate) struct KeyDecoder<'a> {
    actor: RleDecoder<'a, u64>,
    ctr: DeltaDecoder<'a>,
    str: RleDecoder<'a, SmolStr>,
}

impl<'a> KeyDecoder<'a> {
    pub(crate) fn new(actor: RleDecoder<'a, u64>, ctr: DeltaDecoder<'a>, str: RleDecoder<'a, SmolStr>) -> Self {
        Self{ 
            actor,
            ctr,
            str,
        }
    }

    pub(crate) fn done(&self) -> bool {
        self.actor.done() && self.ctr.done() && self.str.done()
    }
}

impl<'a> Iterator for KeyDecoder<'a> {
    type Item = Key;

    fn next(&mut self) -> Option<Key> {
        match (self.actor.next()?, self.ctr.next()?, self.str.next()?) {
            (None, None, Some(string)) => Some(Key::Prop(string)),
            (None, Some(0), None) => Some(Key::Elem(ElemId(OpId(0, 0)))),
            (Some(actor), Some(ctr), None) => {
                Some(Key::Elem(ElemId(OpId(actor, ctr as usize))))
            }
            // TODO: This should be fallible and throw here
            _ => None,
        }
    }
}
