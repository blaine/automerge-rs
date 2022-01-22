use smol_str::SmolStr;

use super::value::CellValue;

#[derive(Debug)]
pub(crate) struct ActionIndex(usize);

impl ActionIndex {
    pub(crate) fn new(val: usize) -> ActionIndex {
        ActionIndex(val)
    }
}

#[derive(Debug)]
pub(crate) struct ActorIndex(usize);

impl ActorIndex {
    pub(crate) fn new(val: usize) -> ActorIndex {
        ActorIndex(val)
    }
}

#[derive(Debug)]
pub(crate) struct OpId(ActorIndex, u64);

impl OpId {
    pub(crate) fn new(act: ActorIndex, ctr: u64) -> OpId {
        OpId(act, ctr)
    }
}

#[derive(Debug)]
pub(crate) enum ObjId {
    Root,
    Op(OpId),
}

#[derive(Debug)]
pub(crate) enum ElemId {
    Head,
    Op(OpId),
}

#[derive(Debug)]
pub(crate) enum Key {
    Prop(SmolStr),
    Elem(ElemId),
}

/// The form operations take in the compressed document format.
#[derive(Debug)]
pub(crate) struct DocOp {
    pub(crate) id: OpId,
    pub(crate) object: ObjId,
    pub(crate) key: Key,
    pub(crate) insert: bool,
    pub(crate) action: ActionIndex,
    pub(crate) value: Option<CellValue>,
    pub(crate) succ: Vec<OpId>,
}
