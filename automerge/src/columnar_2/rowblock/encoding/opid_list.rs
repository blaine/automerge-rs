use crate::types::OpId;

use super::{DeltaDecoder, DeltaEncoder, RleDecoder, RleEncoder, Sink};

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

pub(crate) struct OpIdListEncoder<'a> {
    num: RleEncoder<'a, u64>,
    actor: RleEncoder<'a, u64>,
    ctr: DeltaEncoder<'a>,
}

impl<'a> OpIdListEncoder<'a> {
    pub(crate) fn new(num: RleEncoder<'a, u64>, actor: RleEncoder<'a, u64>, ctr: DeltaEncoder<'a>) -> Self {
        Self{
            num,
            actor,
            ctr,
        }
    }

    pub(crate) fn append(&mut self, opids: Option<&[OpId]>) {
        match opids {
            None | Some(&[]) => self.num.append_value(0),
            Some(opids) => {
                self.num.append_value(opids.len() as u64);
                for opid in opids {
                    self.actor.append_value(opid.actor() as u64);
                    self.ctr.append_value(opid.counter() as u64);
                }
            }
        }
    }
}
