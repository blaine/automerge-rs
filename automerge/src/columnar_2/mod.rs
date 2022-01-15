use std::{io::Read, ops::Range};

mod parse;
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

    fn parse(input: parse::Input) -> parse::ParseResult<ColumnMetadata>  {
        //let mut columns = Vec::new();
        //let mut last_offset = 0;
        //let mut i = input;
        //let mut colspec = [0 as u8;4];
        panic!()
    }
}


pub fn do_the_thing(data: &[u8]) {
    match ColumnMetadata::parse(data) {
        Ok(d) => println!("The data: {:?}", d),
        Err(e) => eprintln!("Error reading the data: {:?}", e),
    };
}
