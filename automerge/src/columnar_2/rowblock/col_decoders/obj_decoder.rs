use crate::{
    columnar_2::rowblock::row_ops::{ActorIndex, ObjId, OpId},
    decoding::RleDecoder,
};

pub(crate) struct ObjDecoder<'a> {
    actor: RleDecoder<'a, usize>,
    ctr: RleDecoder<'a, u64>,
}

impl<'a> ObjDecoder<'a> {
    pub(crate) fn new(actor: RleDecoder<'a, usize>, ctr: RleDecoder<'a, u64>) -> Self {
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
            Some(ObjId::Op(OpId::new(ActorIndex::new(actor), ctr)))
        } else {
            Some(ObjId::Root)
        }
    }
}
