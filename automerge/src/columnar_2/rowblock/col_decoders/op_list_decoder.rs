use crate::{
    columnar_2::rowblock::row_ops::{ActorIndex, OpId},
    decoding::{DeltaDecoder, RleDecoder},
};

pub(crate) struct OpListDecoder<'a> {
    num: RleDecoder<'a, usize>,
    actor: RleDecoder<'a, usize>,
    ctr: DeltaDecoder<'a>,
}

impl<'a> OpListDecoder<'a> {
    pub(crate) fn new(
        num: RleDecoder<'a, usize>,
        actor: RleDecoder<'a, usize>,
        ctr: DeltaDecoder<'a>,
    ) -> Self {
        Self { num, actor, ctr }
    }

    pub(crate) fn done(&self) -> bool {
        self.num.done()
    }
}

impl<'a> Iterator for OpListDecoder<'a> {
    type Item = Vec<OpId>;

    fn next(&mut self) -> Option<Self::Item> {
        let num = self.num.next()??;
        let mut p = Vec::with_capacity(num);
        for _ in 0..num {
            let actor = ActorIndex::new(self.actor.next()??);
            let ctr = self.ctr.next()??;
            p.push(OpId::new(actor, ctr));
        }
        Some(p)
    }
}
