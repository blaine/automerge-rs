use std::{
    borrow::Cow,
    fmt::Debug,
};

use super::{Encodable, Decodable, RawDecoder, Sink};

pub(crate) struct RleEncoder<'a, T>
where
    T: Encodable + PartialEq + Clone,
{
    start_len: usize,
    buf: &'a mut [u8],
    state: RleState<T>,
}

impl<'a, T> RleEncoder<'a, T>
where
    T: Encodable + PartialEq + Clone,
{
    pub fn new(output_buf: &'a mut [u8]) -> RleEncoder<'a, T> {
        RleEncoder {
            start_len: output_buf.len(),
            buf: output_buf,
            state: RleState::Empty,
        }
    }

    pub fn finish(mut self) -> usize {
        match self.take_state() {
            // this covers `only_nulls`
            RleState::NullRun(size) => {
                if !self.buf.is_empty() {
                    self.flush_null_run(size);
                }
            }
            RleState::LoneVal(value) => self.flush_lit_run(vec![value]),
            RleState::Run(value, len) => self.flush_run(&value, len),
            RleState::LiteralRun(last, mut run) => {
                run.push(last);
                self.flush_lit_run(run);
            }
            RleState::Empty => {}
        }
        self.start_len - self.buf.len()
    }

    fn flush_run(&mut self, val: &T, len: usize) {
        self.encode(&(len as i64));
        self.encode(val);
    }

    fn flush_null_run(&mut self, len: usize) {
        self.encode::<i64>(&0);
        self.encode(&len);
    }

    fn flush_lit_run(&mut self, run: Vec<T>) {
        self.encode(&-(run.len() as i64));
        for val in run {
            self.encode(&val);
        }
    }

    fn take_state(&mut self) -> RleState<T> {
        let mut state = RleState::Empty;
        std::mem::swap(&mut self.state, &mut state);
        state
    }

    pub fn append_null(&mut self) {
        self.state = match self.take_state() {
            RleState::Empty => RleState::NullRun(1),
            RleState::NullRun(size) => RleState::NullRun(size + 1),
            RleState::LoneVal(other) => {
                self.flush_lit_run(vec![other]);
                RleState::NullRun(1)
            }
            RleState::Run(other, len) => {
                self.flush_run(&other, len);
                RleState::NullRun(1)
            }
            RleState::LiteralRun(last, mut run) => {
                run.push(last);
                self.flush_lit_run(run);
                RleState::NullRun(1)
            }
        }
    }

    pub fn append_value(&mut self, value: T) {
        self.state = match self.take_state() {
            RleState::Empty => RleState::LoneVal(value),
            RleState::LoneVal(other) => {
                if other == value {
                    RleState::Run(value, 2)
                } else {
                    let mut v = Vec::with_capacity(2);
                    v.push(other);
                    RleState::LiteralRun(value, v)
                }
            }
            RleState::Run(other, len) => {
                if other == value {
                    RleState::Run(other, len + 1)
                } else {
                    self.flush_run(&other, len);
                    RleState::LoneVal(value)
                }
            }
            RleState::LiteralRun(last, mut run) => {
                if last == value {
                    self.flush_lit_run(run);
                    RleState::Run(value, 2)
                } else {
                    run.push(last);
                    RleState::LiteralRun(value, run)
                }
            }
            RleState::NullRun(size) => {
                self.flush_null_run(size);
                RleState::LoneVal(value)
            }
        }
    }

    fn encode<V>(&mut self, val: &V)
    where
        V: Encodable,
    {
        val.encode(&mut self.buf);
    }
}

enum RleState<T> {
    Empty,
    NullRun(usize),
    LiteralRun(T, Vec<T>),
    LoneVal(T),
    Run(T, usize),
}

impl<'a, T: Clone + PartialEq + Encodable> Sink for RleEncoder<'a, T> {
    type Item  = T;

    fn append(&mut self, item: Option<Self::Item>) {
        match item {
            Some(v) => self.append_value(v),
            None => self.append_null(),
        }
    }

    fn finish(self) -> usize {
        self.finish()
    }
}

impl<'a, T: Clone + PartialEq + Encodable> From<&'a mut [u8]> for RleEncoder<'a, T> {
    fn from(output: &'a mut [u8]) -> Self {
        Self::new(output) 
    }
}

/// See discussion on [`RleEncoder`] for the format data is stored in.
#[derive(Debug)]
pub(crate) struct RleDecoder<'a, T> {
    pub decoder: RawDecoder<'a>,
    last_value: Option<T>,
    count: isize,
    literal: bool,
}

impl<'a, T> RleDecoder<'a, T> {
    pub(crate) fn done(&self) -> bool {
        self.decoder.done()
    }
}

impl<'a, T> From<Cow<'a, [u8]>> for RleDecoder<'a, T> {
    fn from(bytes: Cow<'a, [u8]>) -> Self {
        RleDecoder {
            decoder: RawDecoder::from(bytes),
            last_value: None,
            count: 0,
            literal: false,
        }
    }
}

// this decoder needs to be able to send type T or 'null'
// it is an endless iterator that will return all 'null's
// once input is exhausted
impl<'a, T> Iterator for RleDecoder<'a, T>
where
    T: Clone + Debug + Decodable,
{
    type Item = Option<T>;

    fn next(&mut self) -> Option<Option<T>> {
        while self.count == 0 {
            if self.decoder.done() {
                return Some(None);
            }
            match self.decoder.read::<i64>() {
                Ok(count) if count > 0 => {
                    // normal run
                    self.count = count as isize;
                    self.last_value = self.decoder.read().ok();
                    self.literal = false;
                }
                Ok(count) if count < 0 => {
                    // literal run
                    self.count = count.abs() as isize;
                    self.literal = true;
                }
                Ok(_) => {
                    // null run
                    // FIXME(jeffa5): handle usize > i64 here somehow
                    self.count = self.decoder.read::<usize>().unwrap() as isize;
                    self.last_value = None;
                    self.literal = false;
                }
                Err(e) => {
                    tracing::warn!(error=?e, "error during rle decoding");
                    return None;
                }
            }
        }
        self.count -= 1;
        if self.literal {
            Some(self.decoder.read().ok())
        } else {
            Some(self.last_value.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use super::*;

    #[test]
    fn rle_int_round_trip() {
        let vals = [1,1,2,2,3,2,3,1,3];
        let mut buf = vec![0; vals.len() * 3];
        let mut encoder: RleEncoder<'_, u64> = RleEncoder::new(&mut buf);
        for val in vals {
            encoder.append_value(val)
        }
        let total_slice_len = encoder.finish();
        let mut decoder: RleDecoder<'_, u64> = RleDecoder::from(Cow::Borrowed(&buf[0..total_slice_len]));
        let mut result = Vec::new();
        while let Some(Some(val)) = decoder.next() {
            result.push(val);
        }
        assert_eq!(result, vals);
    }

    #[test]
    fn rle_int_insert() {
        let vals = [1,1,2,2,3,2,3,1,3];
        let mut buf = vec![0; vals.len() * 3];
        let mut encoder: RleEncoder<'_, u64> = RleEncoder::new(&mut buf);
        for i in 0..4 {
            encoder.append_value(vals[i])
        }
        encoder.append_value(5);
        for i in 4..vals.len() {
            encoder.append_value(vals[i]);
        }
        let total_slice_len = encoder.finish();
        let mut decoder: RleDecoder<'_, u64> = RleDecoder::from(Cow::Borrowed(&buf[0..total_slice_len]));
        let mut result = Vec::new();
        while let Some(Some(val)) = decoder.next() {
            result.push(val);
        }
        let expected = [1,1,2,2,5,3,2,3,1,3];
        assert_eq!(result, expected);
    }
}
