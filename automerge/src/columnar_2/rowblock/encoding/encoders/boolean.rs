use crate::encoding::Encodable;

/// Encodes booleans by storing the count of the same value.
///
/// The sequence of numbers describes the count of false values on even indices (0-indexed) and the
/// count of true values on odd indices (0-indexed).
///
/// Counts are encoded as usize.
pub(crate) struct BooleanEncoder<'a> {
    start_len: usize,
    buf: &'a mut [u8],
    last: bool,
    count: usize,
}

impl<'a> BooleanEncoder<'a> {
    pub fn new(output: &'a mut [u8]) -> BooleanEncoder<'a> {
        BooleanEncoder {
            start_len: output.len(),
            buf: output,
            last: false,
            count: 0,
        }
    }

    pub fn append(&mut self, value: bool) {
        if value == self.last {
            self.count += 1;
        } else {
            self.count.encode(&mut self.buf).ok();
            self.last = value;
            self.count = 1;
        }
    }

    pub fn finish(mut self) -> usize {
        if self.count > 0 {
            self.count.encode(&mut self.buf).ok();
        }
        self.start_len - self.buf.len()
    }
}

impl<'a> From<&'a mut [u8]> for BooleanEncoder<'a> {
    fn from(output: &'a mut [u8]) -> Self {
        BooleanEncoder::new(output)
    }
}

