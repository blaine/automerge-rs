use  smol_str::SmolStr;

use crate::{
    columnar_2::rowblock::row_ops::{Key, ElemId, ActorIndex, OpId},
    decoding::{RleDecoder, DeltaDecoder},
};


pub(crate) struct KeyDecoder<'a> {
    actor: RleDecoder<'a, usize>,
    ctr: DeltaDecoder<'a>,
    str: RleDecoder<'a, SmolStr>,
}

impl<'a> KeyDecoder<'a> {
    pub(crate) fn new(actor: RleDecoder<'a, usize>, ctr: DeltaDecoder<'a>, str: RleDecoder<'a, SmolStr>) -> Self {
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
            (None, Some(0), None) => Some(Key::Elem(ElemId::Head)),
            (Some(actor), Some(ctr), None) => {
                Some(Key::Elem(ElemId::Op(OpId::new(ActorIndex::new(actor), ctr))))
            }
            // TODO: This should be fallible and throw here
            _ => None,
        }
    }
}
