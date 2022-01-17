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
    String(String),
    Bytes(Vec<u8>),
    Counter(u64),
    Timestamp(u64),
    Unknown { type_code: u8, data: Vec<u8> },
}
