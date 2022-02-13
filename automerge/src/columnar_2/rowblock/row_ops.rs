use smol_str::SmolStr;

use crate::types::{ObjId, ElemId, OpId};
use super::value::PrimVal;

#[derive(Debug)]
pub(crate) enum Key {
    Prop(SmolStr),
    Elem(ElemId),
}

/// The form operations take in the compressed document format.
#[derive(Debug)]
pub(crate) struct DocOp<'a> {
    pub(crate) id: OpId,
    pub(crate) object: ObjId,
    pub(crate) key: Key,
    pub(crate) insert: bool,
    pub(crate) action: usize,
    pub(crate) value: Option<PrimVal<'a>>,
    pub(crate) succ: Vec<OpId>,
}
