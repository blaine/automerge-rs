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
