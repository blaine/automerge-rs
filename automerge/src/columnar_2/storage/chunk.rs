use super::parse;

use std::convert::{TryFrom, TryInto};

const MAGIC_BYTES: [u8; 4] = [0x85, 0x6f, 0x4a, 0x83];

#[derive(Clone, Copy, Debug)]
pub(crate) enum ChunkType {
    Document,
    Change,
    Compressed,
}

impl TryFrom<u8> for ChunkType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Document),
            1 => Ok(Self::Change),
            2 => Ok(Self::Compressed),
            other => Err(other),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CheckSum([u8; 4]);

impl From<[u8; 4]> for CheckSum {
    fn from(raw: [u8; 4]) -> Self {
        CheckSum(raw) 
    }
}

#[derive(Debug)]
pub(crate) struct Chunk<'a> { 
    pub(super) typ: ChunkType,
    pub(super) checksum: CheckSum,
    pub(super) data: &'a [u8],
}

impl<'a> Chunk<'a> {
    pub(crate) fn parse(input: &'a[u8]) -> parse::ParseResult<Chunk<'a>> {
        let (i, magic) = parse::take4(input)?;
        if magic != MAGIC_BYTES {
            return Err(parse::ParseError::Error(parse::ErrorKind::InvalidMagicBytes));
        }
        let (i, checksum_bytes) = parse::take4(i)?;
        let (i, raw_chunk_type) = parse::take1(i)?;
        let chunk_type: ChunkType = raw_chunk_type.try_into().map_err(|e| parse::ParseError::Error(
                parse::ErrorKind::UnknownChunkType(e))
        )?;
        let (i, chunk_len) = parse::leb128_u64(i)?;
        let (i, data) = parse::take_n(chunk_len as usize, i)?;
        Ok((i, Chunk{
            typ: chunk_type,
            checksum: checksum_bytes.into(),
            data,
        }))
    }

    pub(crate) fn typ(&self) -> ChunkType {
        self.typ
    }

    pub(crate) fn checksum(&self) -> CheckSum {
        self.checksum
    }

    pub(crate) fn data(&self) -> &[u8] {
        self.data
    }
}
