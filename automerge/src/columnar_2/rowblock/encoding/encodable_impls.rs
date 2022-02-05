use super::Encodable;

use smol_str::SmolStr;
use std::io::Write;

impl Encodable for SmolStr {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        let bytes = self.as_bytes();
        let head_len = bytes.len().encode(buf);
        buf.write_all(bytes).unwrap();
        head_len + bytes.len() 
    }
}

impl Encodable for String {
    fn encode(&self, buf: &mut Vec<u8>) ->usize {
        let bytes = self.as_bytes();
        let head_len = bytes.len().encode(buf);
        buf.write_all(bytes).unwrap();
        head_len + bytes.len()
    }
}

impl Encodable for Option<String> {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        if let Some(s) = self {
            s.encode(buf)
        } else {
            0.encode(buf)
        }
    }
}

impl Encodable for u64 {
    fn encode(&self, buf: &mut Vec<u8>) -> usize{
        leb128::write::unsigned(buf, *self).unwrap()
    }
}

impl Encodable for f64 {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        let bytes = self.to_le_bytes();
        buf.write_all(&bytes).unwrap();
        bytes.len()
    }
}

impl Encodable for f32 {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        let bytes = self.to_le_bytes();
        buf.write_all(&bytes).unwrap();
        bytes.len()
    }
}

impl Encodable for i64 {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        leb128::write::signed(buf, *self).unwrap()
    }
}

impl Encodable for usize {
    fn encode(&self, buf: &mut Vec<u8>) -> usize{
        (*self as u64).encode(buf)
    }
}

impl Encodable for u32 {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        u64::from(*self).encode(buf)
    }
}

impl Encodable for i32 {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        i64::from(*self).encode(buf)
    }
}

