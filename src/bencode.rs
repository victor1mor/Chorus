#![allow(dead_code)]

use anyhow::{Result, anyhow};
use std::collections::BTreeMap;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BValue {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<BValue>),
    Dict(BTreeMap<Vec<u8>, BValue>),
}

#[derive(Debug, Clone, Copy)]
pub struct DecodeLimits {
    pub max_depth: usize,
    pub max_items: usize,
    pub max_byte_len: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_depth: 128,
            max_items: 1_000_000,
            max_byte_len: 64 * 1024 * 1024, // 64MB max for a single byte string (torrents can be large)
        }
    }
}

pub fn decode(data: &[u8]) -> Result<BValue> {
    decode_with_limits(data, DecodeLimits::default())
}

pub fn decode_with_limits(data: &[u8], limits: DecodeLimits) -> Result<BValue> {
    let (value, pos) = decode_prefix(data, limits)?;
    if pos != data.len() {
        return Err(anyhow!("Trailing garbage after valid bencode"));
    }
    Ok(value)
}

pub fn decode_prefix(data: &[u8], limits: DecodeLimits) -> Result<(BValue, usize)> {
    let mut parser = Parser {
        data,
        pos: 0,
        depth: 0,
        items: 0,
        limits,
    };
    let value = parser.parse_value()?;
    Ok((value, parser.pos))
}

pub fn decode_dict_with_offsets(
    data: &[u8],
    limits: DecodeLimits,
) -> Result<BTreeMap<Vec<u8>, (BValue, usize, usize)>> {
    let mut parser = Parser {
        data,
        pos: 0,
        depth: 0,
        items: 0,
        limits,
    };
    let value = parser.parse_dict_with_offsets()?;
    if parser.pos != data.len() {
        return Err(anyhow!("Trailing garbage after valid bencode"));
    }
    Ok(value)
}

pub fn encode(value: &BValue) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value(value, &mut out);
    out
}

fn encode_value(value: &BValue, out: &mut Vec<u8>) {
    match value {
        BValue::Int(i) => {
            out.push(b'i');
            out.extend_from_slice(i.to_string().as_bytes());
            out.push(b'e');
        }
        BValue::Bytes(b) => {
            out.extend_from_slice(b.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(b);
        }
        BValue::List(l) => {
            out.push(b'l');
            for item in l {
                encode_value(item, out);
            }
            out.push(b'e');
        }
        BValue::Dict(d) => {
            out.push(b'd');
            for (k, v) in d {
                // Key must be bytes
                out.extend_from_slice(k.len().to_string().as_bytes());
                out.push(b':');
                out.extend_from_slice(k);
                encode_value(v, out);
            }
            out.push(b'e');
        }
    }
}

struct Parser<'a> {
    data: &'a [u8],
    pos: usize,
    depth: usize,
    items: usize,
    limits: DecodeLimits,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Result<u8> {
        if self.pos < self.data.len() {
            Ok(self.data[self.pos])
        } else {
            Err(anyhow!("Unexpected end of data"))
        }
    }

    fn advance(&mut self) {
        self.pos += 1;
    }

    fn check_limits(&mut self) -> Result<()> {
        self.items += 1;
        if self.items > self.limits.max_items {
            return Err(anyhow!("Too many items"));
        }
        Ok(())
    }

    fn parse_value(&mut self) -> Result<BValue> {
        self.check_limits()?;
        match self.peek()? {
            b'i' => self.parse_int(),
            b'l' => self.parse_list(),
            b'd' => self.parse_dict(),
            b'0'..=b'9' => self.parse_bytes(),
            _ => Err(anyhow!("Invalid start of value")),
        }
    }

    fn parse_value_with_offsets(&mut self) -> Result<(BValue, usize, usize)> {
        let start = self.pos;
        let val = self.parse_value()?;
        let end = self.pos;
        Ok((val, start, end))
    }

    fn parse_int(&mut self) -> Result<BValue> {
        self.advance(); // consume 'i'
        let start = self.pos;
        while self.peek()? != b'e' {
            self.advance();
        }
        let end = self.pos;
        self.advance(); // consume 'e'

        let num_str = std::str::from_utf8(&self.data[start..end])
            .map_err(|_| anyhow!("Invalid integer encoding (not utf-8)"))?;

        if num_str.is_empty() {
            return Err(anyhow!("Empty integer"));
        }
        if num_str.starts_with("-0") {
            return Err(anyhow!("Negative zero integer"));
        }
        if num_str != "0" && num_str.starts_with('0') {
            return Err(anyhow!("Leading zero in integer"));
        }

        let num = num_str
            .parse::<i64>()
            .map_err(|_| anyhow!("Invalid integer or overflow"))?;
        Ok(BValue::Int(num))
    }

    fn parse_bytes(&mut self) -> Result<BValue> {
        let start = self.pos;
        while self.peek()? != b':' {
            self.advance();
        }
        let end = self.pos;
        self.advance(); // consume ':'

        let len_str = std::str::from_utf8(&self.data[start..end])
            .map_err(|_| anyhow!("Invalid byte length encoding"))?;

        if len_str.starts_with('0') && len_str != "0" {
            return Err(anyhow!("Leading zero in byte length"));
        }

        let len = len_str
            .parse::<usize>()
            .map_err(|_| anyhow!("Invalid byte length"))?;
        if len > self.limits.max_byte_len {
            return Err(anyhow!("Byte string too long"));
        }

        if self.pos + len > self.data.len() {
            return Err(anyhow!("Byte string length exceeds data"));
        }

        let bytes = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(BValue::Bytes(bytes))
    }

    fn parse_list(&mut self) -> Result<BValue> {
        self.advance(); // consume 'l'
        self.depth += 1;
        if self.depth > self.limits.max_depth {
            return Err(anyhow!("Max depth exceeded"));
        }

        let mut list = Vec::new();
        while self.peek()? != b'e' {
            list.push(self.parse_value()?);
        }
        self.advance(); // consume 'e'
        self.depth -= 1;
        Ok(BValue::List(list))
    }

    fn parse_dict(&mut self) -> Result<BValue> {
        self.advance(); // consume 'd'
        self.depth += 1;
        if self.depth > self.limits.max_depth {
            return Err(anyhow!("Max depth exceeded"));
        }

        let mut dict = BTreeMap::new();
        let mut last_key: Option<Vec<u8>> = None;

        while self.peek()? != b'e' {
            let key_val = self.parse_bytes()?;
            let key = match key_val {
                BValue::Bytes(b) => b,
                _ => unreachable!(), // parse_bytes always returns Bytes
            };

            // Keys must be strictly increasing
            if let Some(ref lk) = last_key
                && key <= *lk
            {
                return Err(anyhow!("Dictionary keys not sorted or duplicate"));
            }
            last_key = Some(key.clone());

            let val = self.parse_value()?;
            dict.insert(key, val);
        }
        self.advance(); // consume 'e'
        self.depth -= 1;
        Ok(BValue::Dict(dict))
    }

    fn parse_dict_with_offsets(&mut self) -> Result<BTreeMap<Vec<u8>, (BValue, usize, usize)>> {
        if self.peek()? != b'd' {
            return Err(anyhow!("Expected dictionary"));
        }
        self.advance(); // consume 'd'
        self.depth += 1;
        if self.depth > self.limits.max_depth {
            return Err(anyhow!("Max depth exceeded"));
        }

        let mut dict = BTreeMap::new();
        let mut last_key: Option<Vec<u8>> = None;

        while self.peek()? != b'e' {
            let key_val = self.parse_bytes()?;
            let key = match key_val {
                BValue::Bytes(b) => b,
                _ => unreachable!(),
            };

            // Keys must be strictly increasing
            if let Some(ref lk) = last_key
                && key <= *lk
            {
                return Err(anyhow!("Dictionary keys not sorted or duplicate"));
            }
            last_key = Some(key.clone());

            let val = self.parse_value_with_offsets()?;
            dict.insert(key, val);
        }
        self.advance(); // consume 'e'
        self.depth -= 1;
        Ok(dict)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_int() {
        assert_eq!(decode(b"i42e").unwrap(), BValue::Int(42));
        assert_eq!(decode(b"i-42e").unwrap(), BValue::Int(-42));
        assert_eq!(decode(b"i0e").unwrap(), BValue::Int(0));

        assert!(decode(b"i03e").is_err());
        assert!(decode(b"i-0e").is_err());
        assert!(decode(b"ie").is_err());
        assert!(decode(b"i-e").is_err());
        assert!(decode(b"i99999999999999999999e").is_err()); // overflow
    }

    #[test]
    fn test_decode_bytes() {
        assert_eq!(decode(b"4:spam").unwrap(), BValue::Bytes(b"spam".to_vec()));
        assert_eq!(decode(b"0:").unwrap(), BValue::Bytes(Vec::new()));

        assert!(decode(b"04:spam").is_err());
        assert!(decode(b"4:spa").is_err());
        assert!(decode(b"4:spami").is_err()); // Trailing garbage is caught by top-level
    }

    #[test]
    fn test_decode_list() {
        assert_eq!(
            decode(b"l4:spam4:eggse").unwrap(),
            BValue::List(vec![
                BValue::Bytes(b"spam".to_vec()),
                BValue::Bytes(b"eggs".to_vec())
            ])
        );
        assert_eq!(decode(b"le").unwrap(), BValue::List(Vec::new()));
    }

    #[test]
    fn test_decode_dict() {
        let mut map = BTreeMap::new();
        map.insert(b"cow".to_vec(), BValue::Bytes(b"moo".to_vec()));
        map.insert(b"spam".to_vec(), BValue::Bytes(b"eggs".to_vec()));
        assert_eq!(
            decode(b"d3:cow3:moo4:spam4:eggse").unwrap(),
            BValue::Dict(map)
        );

        // keys not sorted
        assert!(decode(b"d4:spam4:eggs3:cow3:mooe").is_err());
        // duplicate keys
        assert!(decode(b"d3:cow3:moo3:cow4:eggse").is_err());
    }

    #[test]
    fn test_encode() {
        assert_eq!(encode(&BValue::Int(42)), b"i42e");
        assert_eq!(encode(&BValue::Bytes(b"spam".to_vec())), b"4:spam");
        assert_eq!(
            encode(&BValue::List(vec![
                BValue::Bytes(b"spam".to_vec()),
                BValue::Bytes(b"eggs".to_vec())
            ])),
            b"l4:spam4:eggse"
        );

        let mut map = BTreeMap::new();
        map.insert(b"spam".to_vec(), BValue::Bytes(b"eggs".to_vec()));
        map.insert(b"cow".to_vec(), BValue::Bytes(b"moo".to_vec()));
        assert_eq!(encode(&BValue::Dict(map)), b"d3:cow3:moo4:spam4:eggse");
    }

    #[test]
    fn test_trailing_garbage() {
        assert!(decode(b"i42ei42e").is_err());
        assert!(decode(b"4:spambad").is_err());
    }

    #[test]
    fn test_limits() {
        let limits = DecodeLimits {
            max_depth: 2,
            max_items: 10,
            max_byte_len: 10,
        };

        // Depth
        assert!(decode_with_limits(b"lleee", limits).is_err());
        // Items
        assert!(decode_with_limits(b"li1ei2ei3ei4ei5ei6ei7ei8ei9ei10ee", limits).is_err());
        // Byte length
        assert!(decode_with_limits(b"11:hello world", limits).is_err());
    }
}

#[cfg(test)]
mod fuzz_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn fuzz_bencode_parser(data in any::<Vec<u8>>()) {
            let _ = decode(&data);
        }
    }
}
