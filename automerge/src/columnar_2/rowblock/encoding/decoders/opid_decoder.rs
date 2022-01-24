use crate::{
    types::OpId,
    decoding::{RleDecoder, DeltaDecoder},
};


pub(crate) struct OpIdDecoder<'a> {
    actor: RleDecoder<'a, u64>,
    ctr: DeltaDecoder<'a>,
}

impl<'a> OpIdDecoder<'a> {
    pub(crate) fn new(actor: RleDecoder<'a, u64>, ctr: DeltaDecoder<'a>) -> Self {
        Self{ 
            actor,
            ctr,
        }
    }

    pub(crate) fn done(&self) -> bool {
        self.actor.done() && self.ctr.done()
    }
}

impl<'a> Iterator for OpIdDecoder<'a> {
    type Item = OpId;

    fn next(&mut self) -> Option<OpId> {
        match (self.actor.next()?, self.ctr.next()?) {
            (Some(a), Some(c)) => Some(OpId(a, c as usize)),
            // TODO: This should be fallible and throw here
            _ => None,
        }
    }
}
