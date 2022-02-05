mod raw;
pub(crate) use raw::{RawEncoder, RawDecoder};
mod rle;
pub(crate) use rle::{RleEncoder, RleDecoder};
mod boolean;
pub(crate) use boolean::{BooleanDecoder, BooleanEncoder};
mod delta;
pub(crate) use delta::{DeltaDecoder, DeltaEncoder};
mod value;
pub(crate) use value::ValueDecoder;
mod generic;
pub(crate) use generic::GenericColDecoder;
mod opid;
pub(crate) use opid::OpIdDecoder;
mod opid_list;
pub(crate) use opid_list::OpIdListDecoder;
mod obj_id;
pub(crate) use obj_id::ObjDecoder;
mod key;
pub(crate) use key::KeyDecoder;
mod interned_key;
pub(crate) use interned_key::InternedKeyDecoder;



pub(crate) trait Encodable {
    fn encode(&self, buf: &mut [u8]);
}
mod encodable_impls;

pub(crate) trait Decodable: Sized {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: std::io::Read;
}
mod decodable_impls;


pub(crate) trait Sink {
    type Item;

    fn append(&mut self, item: Option<Self::Item>);

    fn finish(self) -> usize;
}

