use crate::encoding::Encodable;
use std::{borrow::Cow, ops::Range};

use smol_str::SmolStr;

use crate::decoding::{BooleanDecoder, Decoder, DeltaDecoder, RleDecoder};

use super::encoding::encoders::{BooleanEncoder, DeltaEncoder, RawEncoder, RleEncoder};

macro_rules! make_col_range({$name: ident, $decoder_name: ident$(<$($dparam: tt),+>)?, $encoder_name: ident$(<$($eparam: tt),+>)?} => {
    #[derive(Clone)]
    pub(crate) struct $name(Range<usize>);

    impl $name {
        pub(crate) fn decoder<'a>(&self, data: &'a[u8]) -> $decoder_name $(<$($dparam,)+>)* {
            $decoder_name::from(Cow::Borrowed(&data[self.0.clone()]))
        }

        pub(crate) fn encoder<'a>(&self, output: &'a mut [u8]) -> $encoder_name $(<$($eparam,)+>)* {
            $encoder_name::from(output)
        }
    }

    impl From<Range<usize>> for $name {
        fn from(r: Range<usize>) -> $name {
            $name(r)
        }
    }
});

make_col_range!(ActorRange, RleDecoder<'a, u64>, RleEncoder<'a, u64>);
make_col_range!(RleIntRange, RleDecoder<'a, u64>, RleEncoder<'a, u64>);
make_col_range!(DeltaIntRange, DeltaDecoder<'a>, DeltaEncoder<'a>);
make_col_range!(RleStringRange, RleDecoder<'a, SmolStr>, RleEncoder<'a, SmolStr>);
make_col_range!(BooleanRange, BooleanDecoder<'a>, BooleanEncoder<'a>);
make_col_range!(RawRange, Decoder<'a>, RawEncoder<'a>);

impl ActorRange {
    fn copy_with_insert(&self, input: &[u8], output: &mut[u8], index: usize, value: u64) -> ActorRange {
        let mut decoder = self.decoder(input);
        let mut encoder = crate::columnar_2::rowblock::encoding::encoders::RleEncoder::new(output);
        for _ in 0..index {
            match  decoder.next().unwrap() {
                Some(v) => encoder.append_value(v),
                None => encoder.append_null(),
            }
        }
        encoder.append_value(value);
        while !decoder.done() {
            match decoder.next().unwrap() {
                Some(v) => encoder.append_value(v),
                None => encoder.append_null(),
            }
        }
        let len = encoder.finish();
        (0..len).into()
    }
}

