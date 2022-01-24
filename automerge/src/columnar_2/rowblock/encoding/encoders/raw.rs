use crate::encoding::Encodable;

pub(crate) struct RawEncoder<'a>{
    start_len: usize,
    output: &'a mut [u8],
}

impl<'a> RawEncoder<'a> {
    pub(crate) fn new(output: &'a mut [u8])  -> Self {
        Self{
            start_len: output.len(),
            output,
        }
    }

    pub(crate) fn append<T: Encodable>(&mut self, val: T){
        val.encode(&mut self.output).unwrap();
    }

    pub(crate) fn finish(self) -> usize {
        self.start_len - self.output.len()
    }
}

impl<'a> From<&'a mut [u8]> for RawEncoder<'a> {
    fn from(output: &'a mut [u8]) -> Self {
        Self::new(output)
    }
}
