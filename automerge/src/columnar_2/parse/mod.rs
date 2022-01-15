use core::num::NonZeroUsize;

mod leb128;

pub(super) type ParseResult<'a, O> = Result<(Input<'a>, O), ParseError<ErrorKind>>;
pub(super) type Input<'a> = &'a [u8];

pub(super) trait Parser<'a, O> {
    fn parse(&mut self, input: Input<'a>) -> ParseResult<'a, O>; 
}

impl<'a, 'b, O, F> Parser<'b, O> for F where F: FnMut(Input<'b>) -> ParseResult<'b, O> + 'a {
    fn parse(&mut self, input: Input<'b>) -> ParseResult<'b, O> {
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
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Leb128TooLarge => write!(f, "invalid leb 128")
        }
    }
}

