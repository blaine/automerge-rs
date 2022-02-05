use crate::types::{OpId, ObjId};

use super::RleDecoder;

pub(crate) struct ObjDecoder<'a> {
    actor: RleDecoder<'a, u64>,
    ctr: RleDecoder<'a, u64>,
}

impl<'a> ObjDecoder<'a> {
    pub(crate) fn new(actor: RleDecoder<'a, u64>, ctr: RleDecoder<'a, u64>) -> Self {
        Self{
            actor,
            ctr,
        }
    }

    pub(crate) fn done(&self) -> bool {
        self.actor.done() || self.ctr.done()
    }
}

impl<'a> Iterator for ObjDecoder<'a> {
    type Item = ObjId;

    fn next(&mut self) -> Option<Self::Item> {
        if let (Some(actor), Some(ctr)) = (self.actor.next()?, self.ctr.next()?) {
            Some(ObjId(OpId(actor, ctr as usize)))
        } else {
            Some(ObjId::root())
        }
    }
}
