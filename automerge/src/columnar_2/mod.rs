mod chunk;
mod column_specification;
mod column_metadata;
mod document;
mod parse;


pub fn do_the_thing(data: &[u8]) {
    match chunk::Chunk::parse(data) {
        Ok((_, d)) => {
            println!("Chunk: {:?}", d);
            match d.typ {
                chunk::ChunkType::Document => {
                    match document::Document::parse(d.data) {
                        Ok((_, doc)) => { println!("Parse document: {:?}", doc); },
                        Err(e) => { eprintln!("Error parsing document: {:?}", e); },
                    }
                },
                _ => println!("It's some other thing"),
            }
        }
        Err(e) => eprintln!("Error reading the data: {:?}", e),
    };
}
