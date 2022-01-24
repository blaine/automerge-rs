use std::convert::TryInto;

use crate::{
    columnar::Action,
    columnar_2::rowblock::{encoding::{
        encoders::Sink,
        decoders::{
            Source, InternedKeyDecoder, OpIdDecoder, OpListDecoder, ValueDecoder,
        },
    },
    column_range::{ActorRange, BooleanRange, DeltaIntRange, RawRange, RleIntRange}},
    decoding::{BooleanDecoder, RleDecoder},
    types::{ObjId, Op},
    ObjType, OpType,
};

use super::ColumnLayout;

/// Similar to DocOpColumns but we don't need the object ID (as we have one op tree per object) and
/// the key_str column is RLE encoded integers as we intern the keys in the OpSet. We also have
/// both pred and succ group columns.
struct OpTreeColumns {
    key_actor: ActorRange,
    key_ctr: DeltaIntRange,
    key_str: RleIntRange,
    id_actor: RleIntRange,
    id_ctr: DeltaIntRange,
    insert: BooleanRange,
    action: RleIntRange,
    val_meta: RleIntRange,
    val_raw: RawRange,
    pred_group: RleIntRange,
    pred_actor: RleIntRange,
    pred_ctr: DeltaIntRange,
    succ_group: RleIntRange,
    succ_actor: RleIntRange,
    succ_ctr: DeltaIntRange,
    change_idx: RleIntRange,
    other: ColumnLayout,
}

impl OpTreeColumns {
    pub(crate) fn insert(&mut self, index: usize, op: Op, data: &[u8]) {
        let mut new_data: Vec<u8> = Vec::with_capacity(data.len() + std::mem::size_of::<Op>());

        // read off index - 1 entries, insert into target slice

        // insert new element
        // read off reamining entries and insert
    }

    pub(crate) fn iter<'a>(&self, obj: ObjId, data: &'a [u8]) -> OpTreeColumnIter<'a> {
        OpTreeColumnIter {
            obj,
            id: OpIdDecoder::new(self.id_actor.decoder(data), self.id_ctr.decoder(data)),
            action: self.action.decoder(data),
            keys: InternedKeyDecoder::new(
                self.key_actor.decoder(data),
                self.key_ctr.decoder(data),
                self.key_str.decoder(data),
            ),
            insert: self.insert.decoder(data),
            value: ValueDecoder::new(
                self.val_meta.decoder(data),
                self.val_raw.decoder(data),
            ),
            pred: OpListDecoder::new(
                self.pred_group.decoder(data),
                self.pred_actor.decoder(data),
                self.pred_ctr.decoder(data),
            ),
            succ: OpListDecoder::new(
                self.succ_group.decoder(data),
                self.succ_actor.decoder(data),
                self.succ_ctr.decoder(data),
            ),
            change_idx: self.change_idx.decoder(data),
        }
    }
}

pub(crate) struct OpTreeColumnIter<'a> {
    obj: ObjId,
    id: OpIdDecoder<'a>,
    action: RleDecoder<'a, u64>,
    keys: InternedKeyDecoder<'a>,
    insert: BooleanDecoder<'a>,
    value: ValueDecoder<'a>,
    pred: OpListDecoder<'a>,
    succ: OpListDecoder<'a>,
    change_idx: RleDecoder<'a, u64>,
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
        ]
        .iter()
        .all(|b| *b)
    }
}

impl<'a> Iterator for OpTreeColumnIter<'a> {
    type Item = Op;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done() {
            None
        } else {
            let action: Action = self.action.next().unwrap().unwrap().try_into().unwrap();
            let action = match action {
                Action::MakeMap => OpType::Make(ObjType::Map),
                Action::MakeTable => OpType::Make(ObjType::Table),
                Action::MakeText => OpType::Make(ObjType::Text),
                Action::MakeList => OpType::Make(ObjType::List),
                Action::Set => OpType::Set(self.value.next().unwrap().into()),
                Action::Inc => OpType::Set(self.value.next().unwrap().into()),
                Action::Del => OpType::Del,
            };
            Some(Op {
                obj: self.obj,
                key: self.keys.next().unwrap(),
                id: self.id.next().unwrap().into(),
                action,
                insert: self.insert.next().unwrap(),
                pred: self.pred.next().unwrap(),
                succ: self.succ.next().unwrap(),
                change: self.change_idx.next().unwrap().unwrap() as usize,
            })
        }
    }
}

fn copy_with_insert<T, I: Source<Item=T>, S: Sink<Item=T>>(mut input: I, mut output: S, index: usize, value: Option<T>) -> usize {
    for _ in 0..index {
        let val = input.next();
        output.append(val);
    }
    output.append(value);
    while !input.done() {
        let val = input.next();
        output.append(val);
    }
    output.finish()
}

fn split_into<T, I: Source<Item=T>, S: Sink<Item=T>>(mut input: I, mut output_one: S, mut output_two: S, index: usize) -> (usize, usize) {
    for _ in 0..index {
        let val = input.next();
        output_one.append(val);
    }
    let size_one = output_one.finish();
    while !input.done() {
        let val = input.next();
        output_two.append(val);
    }
    let size_two = output_two.finish();
    (size_one, size_two)
}
