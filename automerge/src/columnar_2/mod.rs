use std::{io::Read, ops::Range};

mod parse;
mod chunk;
mod column_specification;
use column_specification::ColumnSpec;

struct RowBlock {
    columns: Vec<Column>,
    data: Vec<u8>,
}


#[derive(Debug)]
struct Column {
    spec: ColumnSpec,
    data: Range<u64>,
}

#[derive(Debug)]
struct ColumnMetadata(Vec<Column>);

#[derive(Debug, thiserror::Error)]
enum ParseError {
    #[error("invalid column metadata")]
    InvalidMetadata,
}

impl ColumnMetadata {

    fn parse(input: &[u8]) -> parse::ParseResult<ColumnMetadata>  {
        let mut columns = Vec::new();
        let i = input;
        let (mut i, num_columns) = parse::leb128_u64(i)?;
        let mut current_start = 0;
        for _ in 0..num_columns {
            let (i2, data_len) = parse::leb128_u64(i)?;
            let (i3, spec) = parse::map(parse::read_u32, ColumnSpec::from)(i2)?;
            i = i3;
            let end = current_start + data_len;
            columns.push(Column{ spec, data: current_start..end});
            current_start = end;
        }
        Ok((i, ColumnMetadata(columns)))
    }
}



pub fn do_the_thing(data: &[u8]) {
    match chunk::Chunk::parse(data) {
        Ok(d) => println!("The data: {:?}", d),
        Err(e) => eprintln!("Error reading the data: {:?}", e),
    };
}
