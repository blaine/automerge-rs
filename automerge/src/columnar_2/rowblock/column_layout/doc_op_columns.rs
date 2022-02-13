use std::{convert::TryFrom, ops::Range};

use super::{
    super::{
        super::column_specification::ColumnType,
        column_range::{
            ActorRange, BooleanRange, DeltaIntRange, RawRange, RleIntRange, RleStringRange,
        },
        encoding::{BooleanDecoder, RleDecoder, KeyDecoder, ObjDecoder, OpIdDecoder, OpIdListDecoder, ValueDecoder},
        row_ops::DocOp,
    },
    column::{Column, ColumnRanges, GroupColRange},
    ColumnLayout,
};

pub(crate) struct DocOpColumns {
    actor: ActorRange,
    ctr: RleIntRange,
    key_actor: ActorRange,
    key_ctr: DeltaIntRange,
    key_str: RleStringRange,
    id_actor: RleIntRange,
    id_ctr: DeltaIntRange,
    insert: BooleanRange,
    action: RleIntRange,
    val_meta: RleIntRange,
    val_raw: RawRange,
    succ_group: RleIntRange,
    succ_actor: RleIntRange,
    succ_ctr: DeltaIntRange,
    other: ColumnLayout,
}

impl DocOpColumns {
    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> DocOpColumnIter<'a> {
        DocOpColumnIter {
            id: OpIdDecoder::new(self.id_actor.decoder(data), self.id_ctr.decoder(data)),
            action: self.action.decoder(data),
            objs: ObjDecoder::new(self.actor.decoder(data), self.ctr.decoder(data)),
            keys: KeyDecoder::new(
                self.key_actor.decoder(data),
                self.key_ctr.decoder(data),
                self.key_str.decoder(data),
            ),
            insert: self.insert.decoder(data),
            value: ValueDecoder::new(self.val_meta.decoder(data), self.val_raw.decoder(data)),
            succ: OpIdListDecoder::new(
                self.succ_group.decoder(data),
                self.succ_actor.decoder(data),
                self.succ_ctr.decoder(data),
            ),
        }
    }
}

pub(crate) struct DocOpColumnIter<'a> {
    id: OpIdDecoder<'a>,
    action: RleDecoder<'a, u64>,
    objs: ObjDecoder<'a>,
    keys: KeyDecoder<'a>,
    insert: BooleanDecoder<'a>,
    value: ValueDecoder<'a>,
    succ: OpIdListDecoder<'a>,
}

impl<'a> DocOpColumnIter<'a> {
    fn done(&self) -> bool {
        [
            self.id.done(),
            self.action.done(),
            self.objs.done(),
            self.keys.done(),
            self.insert.done(),
            self.value.done(),
            self.succ.done(),
        ]
        .iter()
        .all(|c| *c)
    }
}

impl<'a> Iterator for DocOpColumnIter<'a> {
    type Item = DocOp<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done() {
            None
        } else {
            let id = self.id.next().unwrap();
            let action = self.action.next().unwrap().unwrap();
            let obj = self.objs.next().unwrap();
            let key = self.keys.next().unwrap();
            let value = self.value.next();
            let succ = self.succ.next().unwrap();
            let insert = self.insert.next().unwrap_or(false);
            Some(DocOp {
                id,
                value,
                action: action as usize,
                object: obj,
                key,
                succ,
                insert,
            })
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("mismatching column at {index}.")]
    MismatchingColumn { index: usize },
    #[error("not enough columns")]
    NotEnoughColumns,
}

impl TryFrom<ColumnLayout> for DocOpColumns {
    type Error = Error;

    fn try_from(columns: ColumnLayout) -> Result<Self, Self::Error> {
        let mut obj_actor: Option<Range<usize>> = None;
        let mut obj_ctr: Option<Range<usize>> = None;
        let mut key_actor: Option<Range<usize>> = None;
        let mut key_ctr: Option<Range<usize>> = None;
        let mut key_str: Option<Range<usize>> = None;
        let mut id_actor: Option<Range<usize>> = None;
        let mut id_ctr: Option<Range<usize>> = None;
        let mut insert: Option<Range<usize>> = None;
        let mut action: Option<Range<usize>> = None;
        let mut val_meta: Option<Range<usize>> = None;
        let mut val_raw: Option<Range<usize>> = None;
        let mut succ_group: Option<Range<usize>> = None;
        let mut succ_actor: Option<Range<usize>> = None;
        let mut succ_ctr: Option<Range<usize>> = None;
        let mut other = ColumnLayout::empty();

        for (index, col) in columns.into_iter().enumerate() {
            match index {
                0 => assert_col_type(index, col, ColumnType::Actor, &mut obj_actor)?,
                1 => assert_col_type(index, col, ColumnType::Integer, &mut obj_ctr)?,
                2 => assert_col_type(index, col, ColumnType::Actor, &mut key_actor)?,
                3 => assert_col_type(index, col, ColumnType::DeltaInteger, &mut key_ctr)?,
                4 => assert_col_type(index, col, ColumnType::String, &mut key_str)?,
                5 => assert_col_type(index, col, ColumnType::Actor, &mut id_actor)?,
                6 => assert_col_type(index, col, ColumnType::DeltaInteger, &mut id_ctr)?,
                7 => assert_col_type(index, col, ColumnType::Boolean, &mut insert)?,
                8 => assert_col_type(index, col, ColumnType::Integer, &mut action)?,
                9 => match col.ranges() {
                    ColumnRanges::Value{meta, val} => {
                        val_meta = Some(meta);
                        val_raw = Some(val);
                    },
                    _ => return Err(Error::MismatchingColumn{ index }),
                },
                10 => match col.ranges() {
                    ColumnRanges::Group{num, mut cols} => {
                        let first = cols.next();
                        let second = cols.next();
                        match (first, second) {
                            (Some(GroupColRange::Single(actor_range)), Some(GroupColRange::Single(ctr_range))) =>
                            {
                                succ_group = Some(num.into());
                                succ_actor = Some(actor_range.into());
                                succ_ctr = Some(ctr_range.into());
                            },
                            _ => return Err(Error::MismatchingColumn{ index }),
                        };
                        if let Some(_) = cols.next() {
                            return Err(Error::MismatchingColumn{ index });
                        }
                    },
                    _ => return Err(Error::MismatchingColumn{ index }),
                },
                _ => {
                    other.append(col);
                }
            }
        }
        Ok(DocOpColumns {
            actor: obj_actor.ok_or(Error::NotEnoughColumns)?.into(),
            ctr: obj_ctr.ok_or(Error::NotEnoughColumns)?.into(),
            key_actor: key_actor.ok_or(Error::NotEnoughColumns)?.into(),
            key_ctr: key_ctr.ok_or(Error::NotEnoughColumns)?.into(),
            key_str: key_str.ok_or(Error::NotEnoughColumns)?.into(),
            id_actor: id_actor.ok_or(Error::NotEnoughColumns)?.into(),
            id_ctr: id_ctr.ok_or(Error::NotEnoughColumns)?.into(),
            insert: insert.ok_or(Error::NotEnoughColumns)?.into(),
            action: action.ok_or(Error::NotEnoughColumns)?.into(),
            val_meta: val_meta.ok_or(Error::NotEnoughColumns)?.into(),
            val_raw: val_raw.ok_or(Error::NotEnoughColumns)?.into(),
            succ_group: succ_group.ok_or(Error::NotEnoughColumns)?.into(),
            succ_actor: succ_actor.ok_or(Error::NotEnoughColumns)?.into(),
            succ_ctr: succ_ctr.ok_or(Error::NotEnoughColumns)?.into(),
            other,
        })
    }
}

fn assert_col_type(
    index: usize,
    col: Column,
    typ: ColumnType,
    target: &mut Option<Range<usize>>,
) -> Result<(), Error> {
    if col.col_type() == typ {
        match col.ranges() {
            ColumnRanges::Single(range) => {
                *target = Some(range);
                Ok(())
            },
            _ => return Err(Error::MismatchingColumn{ index }),
        }
    } else {
        Err(Error::MismatchingColumn { index })
    }
}
