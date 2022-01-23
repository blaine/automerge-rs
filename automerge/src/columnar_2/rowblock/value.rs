use crate::ScalarValue;

use smol_str::SmolStr;

#[derive(Debug)]
pub(crate) enum CellValue {
    Uint(u64),
    Bool(bool),
    String(String),
    Value(PrimVal),
    List(Vec<Vec<CellValue>>),
}

#[derive(Debug)]
pub(crate) enum PrimVal {
    Null,
    Bool(bool),
    Uint(u64),
    Int(i64),
    Float(f64),
    String(SmolStr),
    Bytes(Vec<u8>),
    Counter(u64),
    Timestamp(u64),
    Unknown { type_code: u8, data: Vec<u8> },
}

impl From<PrimVal> for ScalarValue {
    fn from(p: PrimVal) -> Self {
        match p {
            PrimVal::Null => Self::Null,
            PrimVal::Bool(b) => Self::Boolean(b),
            PrimVal::Uint(u) => Self::Uint(u),
            PrimVal::Int(i) => Self::Int(i),
            PrimVal::Float(f) => Self::F64(f),
            PrimVal::String(s) => Self::Str(s),
            PrimVal::Bytes(b) => Self::Bytes(b),
            PrimVal::Counter(c) => Self::Counter((c as i64).into()),
            PrimVal::Timestamp(t) => Self::Timestamp(t as i64),
            PrimVal::Unknown{data, ..} => Self::Bytes(data),
        }
    } 
}
