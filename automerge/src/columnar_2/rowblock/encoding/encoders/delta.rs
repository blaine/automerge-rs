use super::RleEncoder;

/// Encodes integers as the change since the previous value.
///
/// The initial value is 0 encoded as u64. Deltas are encoded as i64.
///
/// Run length encoding is then applied to the resulting sequence.
pub(crate) struct DeltaEncoder<'a> {
    rle: RleEncoder<'a, i64>,
    absolute_value: u64,
}

impl<'a> DeltaEncoder<'a> {
    pub fn new(output: &'a mut [u8]) -> DeltaEncoder<'a> {
        DeltaEncoder {
            rle: RleEncoder::new(output),
            absolute_value: 0,
        }
    }

    pub fn append_value(&mut self, value: u64) {
        self.rle
            .append_value(value as i64 - self.absolute_value as i64);
        self.absolute_value = value;
    }

    pub fn append_null(&mut self) {
        self.rle.append_null();
    }

    pub fn finish(self) -> usize {
        self.rle.finish()
    }
}

impl<'a> From<&'a mut [u8]> for DeltaEncoder<'a> {
    fn from(output: &'a mut [u8]) -> Self {
        DeltaEncoder::new(output) 
    }
}
