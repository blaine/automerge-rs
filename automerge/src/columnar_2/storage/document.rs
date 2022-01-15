use super::{parse, column_metadata::ColumnMetadata};

use crate::{ActorId, ChangeHash};

#[derive(Debug)]
pub(crate) struct Document<'a> {
    actors: Vec<ActorId>,
    heads: Vec<ChangeHash>,
    op_metadata: ColumnMetadata,
    op_bytes: &'a [u8],
    change_metadata: ColumnMetadata,
    change_bytes: &'a [u8],
}

impl<'a> Document<'a> {
    pub(crate) fn parse(input: &'a [u8]) -> parse::ParseResult<Document<'a>> {
        let (i, actors) = parse::length_prefixed(parse::leb128_u64, parse::actor_id)(input)?;
        let (i, heads) = parse::length_prefixed(parse::leb128_u64, parse::change_hash)(i)?;
        let (i, change_meta) = ColumnMetadata::parse(i)?;
        let (i, ops_meta) = ColumnMetadata::parse(i)?;
        let (i, change_data) = parse::take_n(change_meta.total_column_len(), i)?;
        let (i, ops_data) = parse::take_n(ops_meta.total_column_len(), i)?;
        Ok((
            i,
            Document {
                actors,
                heads,
                op_metadata: ops_meta,
                op_bytes: ops_data,
                change_metadata: change_meta,
                change_bytes: change_data,
            },
        ))
    }
}
