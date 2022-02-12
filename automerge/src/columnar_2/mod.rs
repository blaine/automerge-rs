mod column_specification;
mod rowblock;
mod storage;
pub(crate) use column_specification::{ColumnId, ColumnSpec};

pub fn do_the_thing(data: &[u8]) {
    match storage::Chunk::parse(data) {
        Ok((_, d)) => match d.typ() {
            storage::ChunkType::Document => match storage::Document::parse(d.data()) {
                Ok((_, doc)) => {
                    let col_rowblock = rowblock::RowBlock::new(
                        doc.change_metadata.iter(),
                        doc.change_bytes.to_vec(),
                    )
                    .unwrap();
                    println!("Change metadata");
                    for row in &col_rowblock {
                        println!("Row: {:?}", row);
                    }

                    let ops_rowblock =
                        rowblock::RowBlock::new(doc.op_metadata.iter(), doc.op_bytes.to_vec())
                            .unwrap();
                    println!("\n\nOps");
                    for row in &ops_rowblock {
                        println!("Op: {:?}", row);
                    }
                    println!("\n\nSpliced ops");
                    splice_generic_rowblock(&ops_rowblock);
                    let doc_ops_rowblock = ops_rowblock.into_doc_ops().unwrap();
                    for row in &doc_ops_rowblock {
                        println!("{:?}", row);
                    }
                }
                Err(e) => {
                    eprintln!("Error parsing document: {:?}", e);
                }
            },
            storage::ChunkType::Change => match storage::Change::parse(d.data()) {
                Ok((_, change)) => {
                    println!("Parsed change: {:?}", change);
                }
                Err(e) => {
                    eprintln!("Error parsing change: {:?}", e);
                }
            },
            _ => println!("It's some other thing"),
        },
        Err(e) => eprintln!("Error reading the data: {:?}", e),
    };
}

fn splice_generic_rowblock(block: &rowblock::RowBlock<rowblock::ColumnLayout>) {
    use rowblock::{PrimVal, CellValue};
    let spliced = block.splice(2..2, |col_index, _| {
        match col_index {
            0 => None,
            1 => None,
            2 => None,
            3 => None,
            4 => Some(&CellValue::String("other".to_string())),
            5 => Some(&CellValue::Uint(0)),
            6 => Some(&CellValue::Uint(2)),
            7 => Some(&CellValue::Bool(false)),
            8 => Some(&CellValue::Uint(1)),
            9 => Some(&CellValue::Value(PrimVal::Int(2))),
            10 => Some(&CellValue::List(Vec::new())),
            _ => None, 
        }
    }).unwrap();
    for row in &spliced {
        println!("Spliced op: {:?}", row);
    }
}
