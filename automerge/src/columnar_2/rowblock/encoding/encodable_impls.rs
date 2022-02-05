use super::Encodable;

use smol_str::SmolStr;
use std::io::Write;

impl Encodable for SmolStr {
    fn encode(&self, mut buf: &mut [u8]) {
        let bytes = self.as_bytes();
        bytes.len().encode(buf);
        buf.write_all(bytes).unwrap();
    }
}

impl Encodable for String {
    fn encode(&self, mut buf: &mut [u8]) {
        let bytes = self.as_bytes();
        bytes.len().encode(buf);
        buf.write_all(bytes).unwrap();
    }
}

impl Encodable for Option<String> {
    fn encode(&self, buf: &mut [u8]) {
        if let Some(s) = self {
            s.encode(buf)
        } else {
            0.encode(buf)
        }
    }
}

impl Encodable for u64 {
    fn encode(&self, mut buf: &mut [u8]) {
        leb128::write::unsigned(&mut buf, *self).unwrap();
    }
}

impl Encodable for f64 {
    fn encode(&self, mut buf: &mut [u8]) {
        let bytes = self.to_le_bytes();
        buf.write_all(&bytes).unwrap();
    }
}

impl Encodable for f32 {
    fn encode(&self, mut buf: &mut [u8]) {
        let bytes = self.to_le_bytes();
        buf.write_all(&bytes).unwrap();
    }
}

impl Encodable for i64 {
    fn encode(&self, mut buf: &mut [u8]) {
        leb128::write::signed(&mut buf, *self).unwrap();
    }
}

impl Encodable for usize {
    fn encode(&self, buf: &mut [u8]) {
        (*self as u64).encode(buf)
    }
}

impl Encodable for u32 {
    fn encode(&self, buf: &mut [u8]) {
        u64::from(*self).encode(buf)
    }
}

impl Encodable for i32 {
    fn encode(&self, buf: &mut [u8]) {
        i64::from(*self).encode(buf)
    }
}

