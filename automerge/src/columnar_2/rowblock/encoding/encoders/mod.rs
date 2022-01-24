mod rle;
pub(crate) use rle::RleEncoder;
mod delta;
pub(crate) use delta::DeltaEncoder;
mod boolean;
pub(crate) use boolean::BooleanEncoder;
mod raw;
pub(crate) use raw::RawEncoder;


pub(crate) trait Sink {
    type Item;

    fn append(&mut self, item: Option<Self::Item>);

    fn finish(self) -> usize;
}
