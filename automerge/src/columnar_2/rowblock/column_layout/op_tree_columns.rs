use std::{borrow::Cow, ops::Range};

use crate::{
    columnar_2::rowblock::col_decoders::{
        InternedKeyDecoder, OpIdDecoder, OpListDecoder, ValueDecoder,
    },
    decoding::{BooleanDecoder, DeltaDecoder, RleDecoder},
    types::{Op, ObjId}, columnar::Action, OpType, ObjType,
};

use super::ColumnLayout;

/// Similar to DocOpColumns but we don't need the object ID (as we have one op tree per object) and
/// the key_str column is RLE encoded integers as we intern the keys in the OpSet. We also have
/// both pred and succ group columns.
struct OpTreeColumns {
    key_actor: Range<usize>,
    key_ctr: Range<usize>,
    key_str: Range<usize>,
    id_actor: Range<usize>,
    id_ctr: Range<usize>,
    insert: Range<usize>,
    action: Range<usize>,
    val_meta: Range<usize>,
    val_raw: Range<usize>,
    pred_group: Range<usize>,
    pred_actor: Range<usize>,
    pred_ctr: Range<usize>,
    succ_group: Range<usize>,
    succ_actor: Range<usize>,
    succ_ctr: Range<usize>,
    change_idx: Range<usize>,
    other: ColumnLayout,
}

impl OpTreeColumns {
    fn iter<'a>(&self, obj: ObjId, data: &'a [u8]) -> OpTreeColumnIter<'a> {
        OpTreeColumnIter {
            obj,
            id: OpIdDecoder::new(
                RleDecoder::from(Cow::Borrowed(&data[self.id_actor.clone()])),
                DeltaDecoder::from(Cow::Borrowed(&data[self.id_ctr.clone()])),
            ),
            action: RleDecoder::from(Cow::Borrowed(&data[self.action.clone()])),
            keys: InternedKeyDecoder::new(
                RleDecoder::from(Cow::Borrowed(&data[self.key_actor.clone()])),
                DeltaDecoder::from(Cow::Borrowed(&data[self.key_ctr.clone()])),
                RleDecoder::from(Cow::Borrowed(&data[self.key_str.clone()])),
            ),
            insert: BooleanDecoder::from(Cow::Borrowed(&data[self.insert.clone()])),
            value: ValueDecoder::new(&data[self.val_meta.clone()], &data[self.val_raw.clone()]),
            pred: OpListDecoder::new(
                RleDecoder::from(Cow::Borrowed(&data[self.pred_group.clone()])),
                RleDecoder::from(Cow::Borrowed(&data[self.pred_actor.clone()])),
                DeltaDecoder::from(Cow::Borrowed(&data[self.pred_ctr.clone()])),
            ),
            succ: OpListDecoder::new(
                RleDecoder::from(Cow::Borrowed(&data[self.succ_group.clone()])),
                RleDecoder::from(Cow::Borrowed(&data[self.succ_actor.clone()])),
                DeltaDecoder::from(Cow::Borrowed(&data[self.succ_ctr.clone()])),
            ),
            change_idx: RleDecoder::from(Cow::Borrowed(&data[self.change_idx.clone()])),
        }
    }
}

pub(crate) struct OpTreeColumnIter<'a> {
    obj: ObjId,
    id: OpIdDecoder<'a>,
    action: RleDecoder<'a, Action>,
    keys: InternedKeyDecoder<'a>,
    insert: BooleanDecoder<'a>,
    value: ValueDecoder<'a>,
    pred: OpListDecoder<'a>,
    succ: OpListDecoder<'a>,
    change_idx: RleDecoder<'a, usize>,
}

impl<'a> OpTreeColumnIter<'a> {
    fn done(&self) -> bool {
        [
            self.id.done(),
            self.action.done(),
            self.keys.done(),
            self.insert.done(),
            self.pred.done(),
            self.succ.done(),
            self.value.done(),
        ].iter().all(|b| *b)
    }
}

impl<'a> Iterator for OpTreeColumnIter<'a> {
    type Item = Op;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done() {
            None
        } else {
            let action = match self.action.next().unwrap().unwrap() {
                Action::MakeMap => OpType::Make(ObjType::Map),
                Action::MakeTable => OpType::Make(ObjType::Table),
                Action::MakeText => OpType::Make(ObjType::Text),
                Action::MakeList => OpType::Make(ObjType::List),
                Action::Set => OpType::Set(self.value.next().unwrap().into()),
                Action::Inc => OpType::Set(self.value.next().unwrap().into()),
                Action::Del => OpType::Del,
            };
            Some(Op{
                obj: self.obj,
                key: self.keys.next().unwrap(),
                id: self.id.next().unwrap().into(),
                action,
                insert: self.insert.next().unwrap(),
                pred: self.pred.next().unwrap(),
                succ: self.succ.next().unwrap(),
                change: self.change_idx.next().unwrap().unwrap(),
            })
        }
    }
}
