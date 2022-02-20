use std::{borrow::Cow, ops::Range};

use  smol_str::SmolStr;

use crate::{
    types::{ElemId, OpId},
    columnar_2::rowblock::row_ops::Key,
};
use super::{RleDecoder, DeltaDecoder};


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
    
    pub(crate) fn empty() -> KeyDecoder<'static> {
        KeyDecoder {
            actor: RleDecoder::from(Cow::Owned(Vec::new())),
            ctr: DeltaDecoder::from(Cow::Owned(Vec::new())),
            str: RleDecoder::from(Cow::Owned(Vec::new())),
        }
    }

    pub(crate) fn done(&self) -> bool {
        self.actor.done() && self.ctr.done() && self.str.done()
    }

    /// Splice new keys into this set of keys, encoding the resulting actor, counter, and str
    /// columns in `out`. The result is (actor, ctr, str) where actor is the range of the output which
    /// contains the new actor column, ctr the counter column, and str the str column.
    pub(crate) fn splice<'b, I: Iterator<Item = &'b Key> + Clone>(
        &mut self,
        replace: Range<usize>,
        replace_with: I,
        out: &mut Vec<u8>,
    ) -> (Range<usize>, Range<usize>, Range<usize>) {
        panic!()
    }
}

impl<'a> Iterator for KeyDecoder<'a> {
    type Item = Key;

    fn next(&mut self) -> Option<Key> {
        match (self.actor.next(), self.ctr.next(), self.str.next()) {
            (None, None, Some(Some(string))) => Some(Key::Prop(string)),
            (None, Some(Some(0)), None) => Some(Key::Elem(ElemId(OpId(0, 0)))),
            (Some(Some(actor)), Some(Some(ctr)), None) => {
                Some(Key::Elem(ElemId(OpId(actor, ctr as usize))))
            }
            // TODO: This should be fallible and throw here
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use crate::columnar_2::rowblock::encoding::properties::{splice_scenario, row_op_key};

    fn encode(vals: &[Key]) -> (Vec<u8>, Range<usize>, Range<usize>, Range<usize>) {
        let mut out = Vec::new();
        let mut decoder = KeyDecoder::empty();
        let (actor, ctr, string) = decoder.splice(0..0, vals.iter(), &mut out);
        (out, actor, ctr, string)
    }

    proptest!{
        #[test]
        fn splice_key(scenario in splice_scenario(row_op_key())) {
            let (buf, actor, ctr, string) = encode(&scenario.initial_values[..]);
            let mut decoder = KeyDecoder::new(
                RleDecoder::from(&buf[actor]),
                DeltaDecoder::from(&buf[ctr]),
                RleDecoder::from(&buf[string]),
            );
            let mut out = Vec::new();
            let (actor, ctr, string) = decoder.splice(scenario.replace_range.clone(), scenario.replacements.iter(), &mut out);
            let decoder = KeyDecoder::new(
                RleDecoder::from(&buf[actor]),
                DeltaDecoder::from(&buf[ctr]),
                RleDecoder::from(&buf[string.clone()]),
            );
            let result = decoder.collect();
            scenario.check(result);
            assert_eq!(string.end, out.len());
        }
    }
}
