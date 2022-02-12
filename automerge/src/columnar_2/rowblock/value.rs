use crate::ScalarValue;

use std::borrow::Cow;

use smol_str::SmolStr;

#[derive(Debug)]
pub(crate) enum CellValue<'a> {
    Uint(u64),
    Bool(bool),
    String(Cow<'a, SmolStr>),
    Value(PrimVal<'a>),
    List(Vec<Vec<CellValue<'a>>>),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PrimVal<'a> {
    Null,
    Bool(bool),
    Uint(u64),
    Int(i64),
    Float(f64),
    String(Cow<'a, SmolStr>),
    Bytes(Vec<u8>),
    Counter(u64),
    Timestamp(u64),
    Unknown { type_code: u8, data: Vec<u8> },
}

impl<'a> From<PrimVal<'a>> for ScalarValue {
    fn from(p: PrimVal) -> Self {
        match p {
            PrimVal::Null => Self::Null,
            PrimVal::Bool(b) => Self::Boolean(b),
            PrimVal::Uint(u) => Self::Uint(u),
            PrimVal::Int(i) => Self::Int(i),
            PrimVal::Float(f) => Self::F64(f),
            PrimVal::String(s) => Self::Str(s.into()),
            PrimVal::Bytes(b) => Self::Bytes(b),
            PrimVal::Counter(c) => Self::Counter((c as i64).into()),
            PrimVal::Timestamp(t) => Self::Timestamp(t as i64),
            PrimVal::Unknown{data, ..} => Self::Bytes(data),
        }
    } 
}
