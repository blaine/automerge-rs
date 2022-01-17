use crate::{ActorId, ChangeHash};

use super::{parse, ColumnMetadata};

#[derive(Debug)]
pub(crate) struct Change<'a> {
    dependencies: Vec<ChangeHash>,
    actor: ActorId,
    other_actors: Vec<ActorId>,
    seq: u64,
    start_op: u64,
    timestamp: u64,
    message: Option<String>,
    ops_meta: ColumnMetadata,
    ops_data: &'a [u8],
    extra_bytes: &'a [u8],
}

impl<'a> Change<'a> {
    pub(crate) fn parse(input: &'a [u8]) -> parse::ParseResult<Change<'a>> {
        let (i, deps) = parse::length_prefixed(parse::leb128_u64, parse::change_hash)(input)?;
        let (i, actor) = parse::actor_id(i)?;
        let (i, seq) = parse::leb128_u64(i)?;
        let (i, start_op) = parse::leb128_u64(i)?;
        let (i, timestamp) = parse::leb128_u64(i)?;
        let (i, message_len) = parse::leb128_u64(i)?;
        let (i, message) = parse::utf_8(message_len as usize, i)?;
        let (i, other_actors) = parse::length_prefixed(parse::leb128_u64, parse::actor_id)(i)?;
        let (i, ops_meta) = ColumnMetadata::parse(i)?;
        let (i, ops_data) = parse::take_n(ops_meta.total_column_len(), i)?;
        Ok((
            &[],
            Change {
                dependencies: deps,
                actor,
                other_actors,
                seq,
                start_op,
                timestamp,
                message: if message.is_empty() {
                    None
                } else {
                    Some(message)
                },
                ops_meta,
                ops_data,
                extra_bytes: i,
            },
        ))
    }
}
