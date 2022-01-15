mod storage;

pub fn do_the_thing(data: &[u8]) {
    match storage::Chunk::parse(data) {
        Ok((_, d)) => {
            println!("Chunk: {:?}", d);
            match d.typ() {
                storage::ChunkType::Document => {
                    match storage::Document::parse(d.data()) {
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
