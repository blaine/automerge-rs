use crate::types::OpId;

use super::{RleEncoder, RleDecoder, DeltaEncoder, DeltaDecoder};


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
        match (self.actor.next(), self.ctr.next()) {
            (Some(Some(a)), Some(c)) => Some(OpId(a, c as usize)),
            // TODO: This should be fallible and throw here
            _ => None,
        }
    }
}

pub(crate) struct OpIdEncoder<'a> {
    actor: RleEncoder<'a, u64>,
    ctr: DeltaEncoder<'a>,
}

impl<'a> OpIdEncoder<'a> {
    fn new(actor: RleEncoder<'a, u64>, ctr: DeltaEncoder<'a>) -> Self {
        Self{
            actor,
            ctr,
        }
    }

    fn append_value(&mut self, opid: Option<OpId>) {
        match opid {
            None => {
                self.actor.append_null();
                self.ctr.append_null();
            },
            Some(opid) => {
                self.actor.append_value(opid.actor() as u64);
                self.ctr.append_value(opid.counter());
            }
        }
    }
}

