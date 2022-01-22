use std::convert::TryFrom;

use crate::{
    columnar::Action,
    decoding::{BooleanDecoder, Decoder, DeltaDecoder, RleDecoder},
};

use super::{
    column::{Column, CopyRange, GroupedColumn, SimpleColType},
    ColumnLayout,
};

use smol_str::SmolStr;

struct Actor(usize);

struct KeyIterator<'a> {
    actor: RleDecoder<'a, usize>,
    ctr: DeltaDecoder<'a>,
    str: RleDecoder<'a, SmolStr>,
}

struct ValueIterator<'a> {
    val_len: RleDecoder<'a, usize>,
    val_raw: Decoder<'a>,
    actor: RleDecoder<'a, usize>,
    ctr: RleDecoder<'a, u64>,
}

struct ObjIterator<'a> {
    actor: RleDecoder<'a, usize>,
    ctr: RleDecoder<'a, u64>,
}

struct SuccIterator<'a> {
    succ_num: RleDecoder<'a, usize>,
    succ_actor: RleDecoder<'a, usize>,
    succ_ctr: DeltaDecoder<'a>,
}

struct DocOpColumns {
    actor: CopyRange<usize>,
    ctr: CopyRange<usize>,
    key_actor: CopyRange<usize>,
    key_ctr: CopyRange<usize>,
    key_str: CopyRange<usize>,
    id_actor: CopyRange<usize>,
    id_ctr: CopyRange<usize>,
    insert: CopyRange<usize>,
    action: CopyRange<usize>,
    val_meta: CopyRange<usize>,
    val_raw: CopyRange<usize>,
    succ_group: CopyRange<usize>,
    succ_actor: CopyRange<usize>,
    succ_ctr: CopyRange<usize>,
    other: ColumnLayout,
}

struct DocOpColumnIter<'a> {
    actor: RleDecoder<'a, usize>,
    ctr: DeltaDecoder<'a>,
    action: RleDecoder<'a, Action>,
    objs: ObjIterator<'a>,
    keys: KeyIterator<'a>,
    insert: BooleanDecoder<'a>,
    value: ValueIterator<'a>,
    succ: SuccIterator<'a>,
    other: ColumnLayout,
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("mismatching column at {index}.")]
    MismatchingColumn { index: usize },
    #[error("not enough columns")]
    NotEnoughColumns,
}

impl TryFrom<ColumnLayout> for DocOpColumns {
    type Error = Error;

    fn try_from(columns: ColumnLayout) -> Result<Self, Self::Error> {
        let mut obj_actor: Option<CopyRange<usize>> = None;
        let mut obj_ctr: Option<CopyRange<usize>> = None;
        let mut key_actor: Option<CopyRange<usize>> = None;
        let mut key_ctr: Option<CopyRange<usize>> = None;
        let mut key_str: Option<CopyRange<usize>> = None;
        let mut id_actor: Option<CopyRange<usize>> = None;
        let mut id_ctr: Option<CopyRange<usize>> = None;
        let mut insert: Option<CopyRange<usize>> = None;
        let mut action: Option<CopyRange<usize>> = None;
        let mut val_meta: Option<CopyRange<usize>> = None;
        let mut val_raw: Option<CopyRange<usize>> = None;
        let mut succ_group: Option<CopyRange<usize>> = None;
        let mut succ_actor: Option<CopyRange<usize>> = None;
        let mut succ_ctr: Option<CopyRange<usize>> = None;
        let mut other = ColumnLayout::empty();

        for (index, col) in columns.into_iter().enumerate() {
            match index {
                0 => assert_simple_col(index, col, SimpleColType::Actor, &mut obj_actor)?,
                1 => assert_simple_col(index, col, SimpleColType::Integer, &mut obj_ctr)?,
                2 => assert_simple_col(index, col, SimpleColType::Actor, &mut key_actor)?,
                3 => assert_simple_col(index, col, SimpleColType::DeltaInteger, &mut key_ctr)?,
                4 => assert_simple_col(index, col, SimpleColType::String, &mut key_str)?,
                5 => assert_simple_col(index, col, SimpleColType::Actor, &mut id_actor)?,
                6 => assert_simple_col(index, col, SimpleColType::DeltaInteger, &mut id_ctr)?,
                7 => assert_simple_col(index, col, SimpleColType::Boolean, &mut insert)?,
                8 => assert_simple_col(index, col, SimpleColType::Integer, &mut action)?,
                9 => match col {
                    Column::Single(..) => return Err(Error::MismatchingColumn { index }),
                    Column::Value { meta, value, .. } => {
                        val_meta = Some(meta);
                        val_raw = Some(value);
                    }
                    Column::Group { .. } => return Err(Error::MismatchingColumn { index }),
                },
                10 => match col {
                    Column::Single(..) => return Err(Error::MismatchingColumn { index }),
                    Column::Value { .. } => return Err(Error::MismatchingColumn { index }),
                    Column::Group { num, values, .. } => match &values[..] {
                        &[GroupedColumn::Single(_, SimpleColType::Actor, actor_range), GroupedColumn::Single(_, SimpleColType::DeltaInteger, ctr_range)] =>
                        {
                            succ_group = Some(num);
                            succ_actor = Some(actor_range);
                            succ_ctr = Some(ctr_range);
                        }
                        _ => return Err(Error::MismatchingColumn { index }),
                    },
                },
                _ => {
                    other.append(col);
                }
            }
        }
        Ok(DocOpColumns {
            actor: obj_actor.ok_or(Error::NotEnoughColumns)?,
            ctr: obj_ctr.ok_or(Error::NotEnoughColumns)?,
            key_actor: key_actor.ok_or(Error::NotEnoughColumns)?,
            key_ctr: key_ctr.ok_or(Error::NotEnoughColumns)?,
            key_str: key_str.ok_or(Error::NotEnoughColumns)?,
            id_actor: id_actor.ok_or(Error::NotEnoughColumns)?,
            id_ctr: id_ctr.ok_or(Error::NotEnoughColumns)?,
            insert: insert.ok_or(Error::NotEnoughColumns)?,
            action: action.ok_or(Error::NotEnoughColumns)?,
            val_meta: val_meta.ok_or(Error::NotEnoughColumns)?,
            val_raw: val_raw.ok_or(Error::NotEnoughColumns)?,
            succ_group: succ_group.ok_or(Error::NotEnoughColumns)?,
            succ_actor: succ_actor.ok_or(Error::NotEnoughColumns)?,
            succ_ctr: succ_ctr.ok_or(Error::NotEnoughColumns)?,
            other,
        })
    }
}

fn assert_simple_col(
    index: usize,
    col: Column,
    typ: SimpleColType,
    target: &mut Option<CopyRange<usize>>,
) -> Result<(), Error> {
    match col {
        Column::Single(_, this_typ, range) if this_typ == typ => {
            *target = Some(range);
            Ok(())
        }
        _ => Err(Error::MismatchingColumn { index }),
    }
}
