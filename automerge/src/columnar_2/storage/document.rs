use super::{column_metadata::ColumnMetadata, parse};

use crate::{ActorId, ChangeHash};

#[derive(Debug)]
pub(crate) struct Document<'a> {
    pub(crate) actors: Vec<ActorId>,
    pub(crate) heads: Vec<ChangeHash>,
    pub(crate) op_metadata: ColumnMetadata,
    pub(crate) op_bytes: &'a [u8],
    pub(crate) change_metadata: ColumnMetadata,
    pub(crate) change_bytes: &'a [u8],
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
