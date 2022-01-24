pub mod decoders;
pub mod encoders;

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use super::*;

    #[test]
    fn rle_int_round_trip() {
        let vals = [1,1,2,2,3,2,3,1,3];
        let mut buf = vec![0; vals.len() * 3];
        let mut encoder: encoders::RleEncoder<'_, u64> = encoders::RleEncoder::new(&mut buf);
        for val in vals {
            encoder.append_value(val)
        }
        let total_slice_len = encoder.finish();
        let mut decoder: decoders::RleDecoder<'_, u64> = decoders::RleDecoder::from(Cow::Borrowed(&buf[0..total_slice_len]));
        let mut result = Vec::new();
        while let Some(Some(val)) = decoder.next() {
            result.push(val);
        }
        assert_eq!(result, vals);
    }

    #[test]
    fn rle_int_insert() {
        let vals = [1,1,2,2,3,2,3,1,3];
        let mut buf = vec![0; vals.len() * 3];
        let mut encoder: encoders::RleEncoder<'_, u64> = encoders::RleEncoder::new(&mut buf);
        for i in 0..4 {
            encoder.append_value(vals[i])
        }
        encoder.append_value(5);
        for i in 4..vals.len() {
            encoder.append_value(vals[i]);
        }
        let total_slice_len = encoder.finish();
        let mut decoder: decoders::RleDecoder<'_, u64> = decoders::RleDecoder::from(Cow::Borrowed(&buf[0..total_slice_len]));
        let mut result = Vec::new();
        while let Some(Some(val)) = decoder.next() {
            result.push(val);
        }
        let expected = [1,1,2,2,5,3,2,3,1,3];
        assert_eq!(result, expected);
    }
}
