use super::Sink;
use crate::encoding::Encodable;

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
        val.encode(&mut self.buf).unwrap();
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
