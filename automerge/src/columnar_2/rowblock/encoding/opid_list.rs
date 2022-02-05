use crate::types::OpId;

use super::{DeltaDecoder, RleDecoder};

pub(crate) struct OpIdListDecoder<'a> {
    num: RleDecoder<'a, u64>,
    actor: RleDecoder<'a, u64>,
    ctr: DeltaDecoder<'a>,
}

impl<'a> OpIdListDecoder<'a> {
    pub(crate) fn new(
        num: RleDecoder<'a, u64>,
        actor: RleDecoder<'a, u64>,
        ctr: DeltaDecoder<'a>,
    ) -> Self {
        Self { num, actor, ctr }
    }

    pub(crate) fn done(&self) -> bool {
        self.num.done()
    }
}

impl<'a> Iterator for OpIdListDecoder<'a> {
    type Item = Vec<OpId>;

    fn next(&mut self) -> Option<Self::Item> {
        let num = self.num.next()??;
        let mut p = Vec::with_capacity(num as usize);
        for _ in 0..num {
            let actor = self.actor.next()??;
            let ctr = self.ctr.next()??;
            p.push(OpId(actor, ctr as usize));
        }
        Some(p)
    }
}
