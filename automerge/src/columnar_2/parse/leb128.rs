use core::{mem::size_of, num::NonZeroUsize};

use super::{ErrorKind, Input, Needed, ParseError, ParseResult};

/// Recognizes an leb128 encoded integer which can fit in a u64
pub(in crate::columnar_2) fn leb128_u64<'a>(input: Input<'a>) -> ParseResult<'a, u64> {
    let mut res = 0;
    let mut shift = 0;

    for (pos, byte) in input.iter().enumerate() {
        if (byte & 0x80) == 0 {
            res |= (*byte as u64) << shift;
            return Ok((&input[pos + 1..], res));
        } else if pos == leb128_size::<u64>() - 1 {
            return Err(ParseError::Error(ErrorKind::Leb128TooLarge));
        } else {
            res |= ((byte & 0x7F) as u64) << shift;
        }
        shift += 7;
    }
    Err(ParseError::Incomplete(NEED_ONE))
}

/// Maximum LEB128-encoded size of an integer type
const fn leb128_size<T>() -> usize {
    let bits = size_of::<T>() * 8;
    (bits + 6) / 7 // equivalent to ceil(bits/7) w/o floats
}

const NEED_ONE: Needed = Needed::Size(unsafe { NonZeroUsize::new_unchecked(1) });

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leb_128_u64() {
        let scenarios: Vec<(&'static [u8], ParseResult<u64>)> = vec![
            (&[0b00000001_u8], Ok((&[], 1))),
            (&[0b10000001_u8], Err(ParseError::Incomplete(NEED_ONE))),
            (&[0b10000001, 0b00000001], Ok((&[], 129))),
            (&[0b00000001, 0b00000011], Ok((&[0b00000011], 1))),
            (
                &[129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129],
                Err(ParseError::Error(ErrorKind::Leb128TooLarge)),
            ),
        ];
        for (index, (input, expected)) in scenarios.into_iter().enumerate() {
            let result = leb128_u64(input);
            if result != expected {
                panic!(
                    "Scenario {} failed: expected {:?} got {:?}",
                    index + 1,
                    expected,
                    result
                );
            }
        }
    }
}
