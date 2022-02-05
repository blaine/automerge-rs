use std::{borrow::Cow, ops::Range};

use smol_str::SmolStr;

use super::encoding::{
    BooleanDecoder, BooleanEncoder, DeltaDecoder, DeltaEncoder, RawDecoder, RawEncoder,
    RleDecoder, RleEncoder,
};

macro_rules! make_col_range({$name: ident, $decoder_name: ident$(<$($dparam: tt),+>)?, $encoder_name: ident$(<$($eparam: tt),+>)?} => {
    #[derive(Clone)]
    pub(crate) struct $name(Range<usize>);

    impl $name {
        pub(crate) fn decoder<'a>(&self, data: &'a[u8]) -> $decoder_name $(<$($dparam,)+>)* {
            $decoder_name::from(Cow::Borrowed(&data[self.0.clone()]))
        }

        pub(crate) fn encoder<'a>(&self, output: &'a mut Vec<u8>) -> $encoder_name $(<$($eparam,)+>)* {
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
make_col_range!(
    RleStringRange,
    RleDecoder<'a, SmolStr>,
    RleEncoder<'a, SmolStr>
);
make_col_range!(BooleanRange, BooleanDecoder<'a>, BooleanEncoder<'a>);
make_col_range!(RawRange, RawDecoder<'a>, RawEncoder<'a>);
