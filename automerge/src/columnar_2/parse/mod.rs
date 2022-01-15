use core::num::NonZeroUsize;
use std::{convert::TryInto, marker::PhantomData};

mod leb128;
pub(in crate::columnar_2) use self::leb128::leb128_u64;

pub(super) type ParseResult<'a, O> = Result<(Input<'a>, O), ParseError<ErrorKind>>;
pub(super) type Input<'a> = &'a [u8];

pub(super) trait Parser<'a, O> {
    fn parse(&mut self, input: Input<'a>) -> ParseResult<'a, O>; 

    fn map<G, O2>(self, g: G) -> Map<Self, G, O> 
        where 
            G: FnMut(O) -> O2,
            Self: Sized
    {
        Map { f: self, g, _phantom: PhantomData }
    }
}

pub(super) struct Map<F, G, O1> {
    f: F,
    g: G,
    _phantom: PhantomData<O1>
}

impl<'a, O1, O2, F: Parser<'a, O1>, G: Fn(O1) -> O2> Parser<'a, O2> for Map<F, G, O1> {
    fn parse(&mut self, input: Input<'a>) -> ParseResult<'a, O2> {
        match self.f.parse(input) {
            Err(e) => Err(e),
            Ok((i, o)) => Ok((i, (self.g)(o))),
        }
    }
}

impl<'a, O, F> Parser<'a, O> for F where F: FnMut(Input<'a>) -> ParseResult<'a, O> {
    fn parse(&mut self, input: Input<'a>) -> ParseResult<'a, O> {
        (self)(input)
    }
}

#[derive(Debug, PartialEq)]
pub(super) enum ParseError<E> {
    Error(E),
    Incomplete(Needed),
}

#[derive(Debug, PartialEq)]
pub(super) enum Needed {
    Unknown,
    Size(NonZeroUsize),
}

#[derive(Debug, PartialEq)]
pub(super) enum ErrorKind {
    Leb128TooLarge,
    InvalidMagicBytes,
    UnknownChunkType(u8),
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Leb128TooLarge => write!(f, "invalid leb 128"),
            Self::InvalidMagicBytes => write!(f, "invalid magic bytes"),
            Self::UnknownChunkType(t) => write!(f, "unknown chunk type: {}", t),
        }
    }
}

pub(super) fn map<'a, O1, O2, F, G>(mut parser: F, mut f: G) -> impl FnMut(&'a [u8]) -> ParseResult<'a, O2>
where
  F: Parser<'a, O1>,
  G: FnMut(O1) -> O2,
{
  move |input: &[u8]| {
    let (input, o1) = parser.parse(input)?;
    Ok((input, f(o1)))
  }
}

pub(super) fn read_u32<'a>(input: &'a [u8]) -> ParseResult<u32> {
    map(take4, u32::from_be_bytes)(input)
}

pub(super) fn take1(input: &[u8]) -> ParseResult<u8> {
    if let Some(need) = NonZeroUsize::new(1_usize.saturating_sub(input.len())) {
        Err(ParseError::Incomplete(Needed::Size(need)))
    } else {
        let (result, remaining) = input.split_at(1);
        Ok((remaining, result[0]))
    }
}

pub(super) fn take4(input: &[u8]) -> ParseResult<[u8; 4]> {
    if let Some(need) = NonZeroUsize::new(4_usize.saturating_sub(input.len())) {
        Err(ParseError::Incomplete(Needed::Size(need)))
    } else {
        let (result, remaining) = input.split_at(4);
        Ok((remaining, result.try_into().expect("we checked the length")))
    }
}

pub(super) fn take_n<'a>(n: usize, input: &'a[u8]) -> ParseResult<&'a[u8]>{
    if let Some(need) = NonZeroUsize::new(n.saturating_sub(input.len())) {
        Err(ParseError::Incomplete(Needed::Size(need)))
    } else {
        Ok(input.split_at(n))
    }
}
