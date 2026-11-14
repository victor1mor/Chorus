#![allow(dead_code)]
use crate::bencode::{self, BValue, DecodeLimits};
use anyhow::{Result, anyhow};
use sha1::{Digest, Sha1};
use std::path::{Component, Path};

#[derive(Debug, Clone)]
pub struct Metainfo {
    pub announce: Option<String>,
    pub announce_list: Option<Vec<Vec<String>>>,
    pub info_hash: [u8; 20],
    pub info: Info,
    pub private: bool,
    pub nodes: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum Info {
    SingleFile(SingleFileInfo),
    MultiFile(MultiFileInfo),
}

#[derive(Debug, Clone)]
pub struct SingleFileInfo {
    pub name: String,
    pub piece_length: usize,
    pub pieces: Vec<[u8; 20]>,
    pub length: usize,
}

#[derive(Debug, Clone)]
pub struct MultiFileInfo {
    pub name: String,
    pub piece_length: usize,
    pub pieces: Vec<[u8; 20]>,
    pub files: Vec<FileInfo>,
}

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub length: usize,
    pub path: Vec<String>,
}

impl Metainfo {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let limits = DecodeLimits::default();
        let dict = bencode::decode_dict_with_offsets(data, limits)
            .map_err(|e| anyhow!("Failed to decode metainfo dict: {}", e))?;

        let mut announce = None;
        if let Some((BValue::Bytes(b), _, _)) = dict.get(b"announce".as_slice()) {
            announce = String::from_utf8(b.clone()).ok();
        }

        let mut announce_list = None;
        if let Some((BValue::List(l), _, _)) = dict.get(b"announce-list".as_slice()) {
            let mut lists = Vec::new();
            for item in l {
                if let BValue::List(inner) = item {
                    let mut inner_list = Vec::new();
                    for inner_item in inner {
                        if let BValue::Bytes(b) = inner_item
                            && let Ok(s) = String::from_utf8(b.clone())
                        {
                            inner_list.push(s);
                        }
                    }
                    if !inner_list.is_empty() {
                        lists.push(inner_list);
                    }
                }
            }
            if !lists.is_empty() {
                announce_list = Some(lists);
            }
        }

        let (info_val, info_start, info_end) = dict
            .get(b"info".as_slice())
            .ok_or_else(|| anyhow!("Missing info dictionary"))?;
        let BValue::Dict(info_dict) = info_val else {
            return Err(anyhow!("info is not a dictionary"));
        };

        let info = Info::from_bvalue_dict(info_dict)?;
        let pieces = match &info {
            Info::SingleFile(single) => &single.pieces,
            Info::MultiFile(multi) => &multi.pieces,
        };
        if pieces.is_empty() {
            return Err(anyhow!("torrent total length must be positive"));
        }

        let nodes = match dict.get(b"nodes".as_slice()) {
            Some((BValue::List(nodes), _, _)) => nodes
                .iter()
                .filter_map(|node| {
                    let BValue::List(pair) = node else {
                        return None;
                    };
                    let [BValue::Bytes(host), BValue::Int(port)] = pair.as_slice() else {
                        return None;
                    };
                    let host = std::str::from_utf8(host).ok()?;
                    let port = u16::try_from(*port).ok()?;
                    (!host.is_empty() && !host.contains(':') && port != 0)
                        .then(|| format!("{host}:{port}"))
                })
                .take(64)
                .collect(),
            _ => Vec::new(),
        };
        Ok(Metainfo {
            announce,
            announce_list,
            info_hash: Sha1::digest(&data[*info_start..*info_end]).into(),
            info,
            private: get_int_opt(info_dict, b"private") == Some(1),
            nodes,
        })
    }

    pub fn tracker_urls(&self) -> impl Iterator<Item = &str> {
        self.announce
            .iter()
            .chain(self.announce_list.iter().flatten().flatten())
            .map(String::as_str)
    }
}

impl Info {
    pub fn from_bytes(data: &[u8], expected_hash: &[u8; 20]) -> Result<Self> {
        let mut hasher = Sha1::new();
        hasher.update(data);
        let hash: [u8; 20] = hasher.finalize().into();

        if &hash != expected_hash {
            return Err(anyhow!("info hash mismatch"));
        }

        let limits = DecodeLimits::default();
        let val = bencode::decode_with_limits(data, limits)
            .map_err(|e| anyhow!("Failed to decode info dict: {}", e))?;

        let dict = match val {
            BValue::Dict(d) => d,
            _ => return Err(anyhow!("info is not a dictionary")),
        };

        Self::from_bvalue_dict(&dict)
    }

    fn from_bvalue_dict(info_dict: &std::collections::BTreeMap<Vec<u8>, BValue>) -> Result<Self> {
        if get_int_opt(info_dict, b"meta version") == Some(2)
            && !info_dict.contains_key(b"pieces".as_slice())
        {
            return Err(anyhow!("BitTorrent v2-only torrents are unsupported"));
        }
        let name = get_string(info_dict, b"name")?;
        validate_name(&name)?;

        let piece_length = get_int(info_dict, b"piece length")?;
        if !(16 * 1024..=64 * 1024 * 1024).contains(&piece_length) {
            return Err(anyhow!("piece length must be between 16 KiB and 64 MiB"));
        }
        let piece_length = piece_length as usize;

        let pieces_bytes = get_bytes(info_dict, b"pieces")?;
        if pieces_bytes.len() % 20 != 0 {
            return Err(anyhow!("pieces length is not a multiple of 20"));
        }
        let num_pieces = pieces_bytes.len() / 20;
        let mut pieces = Vec::with_capacity(num_pieces);
        for chunk in pieces_bytes.as_chunks::<20>().0 {
            let mut arr = [0u8; 20];
            arr.copy_from_slice(chunk);
            pieces.push(arr);
        }

        let length = get_int_opt(info_dict, b"length");
        let files = get_list_opt(info_dict, b"files");

        let info = match (length, files) {
            (Some(len), None) => {
                let len = usize::try_from(len).map_err(|_| anyhow!("invalid file length"))?;
                let expected_pieces = len.div_ceil(piece_length);
                if num_pieces != expected_pieces {
                    return Err(anyhow!("pieces count mismatch for single-file"));
                }
                Info::SingleFile(SingleFileInfo {
                    name,
                    piece_length,
                    pieces,
                    length: len,
                })
            }
            (None, Some(files_list)) => {
                let mut parsed_files = Vec::new();
                let mut total_length: usize = 0;
                let mut seen_paths = std::collections::HashSet::new();

                for f in files_list {
                    let f_dict = match f {
                        BValue::Dict(d) => d,
                        _ => return Err(anyhow!("files item is not a dictionary")),
                    };

                    let flen = get_int(f_dict, b"length")?;
                    let flen = usize::try_from(flen).map_err(|_| anyhow!("invalid file length"))?;
                    total_length = total_length
                        .checked_add(flen)
                        .ok_or_else(|| anyhow!("total length overflow"))?;

                    let path_list = get_list(f_dict, b"path")?;
                    if path_list.is_empty() {
                        return Err(anyhow!("empty path list"));
                    }

                    let mut parsed_path = Vec::new();
                    for p in path_list {
                        let b = match p {
                            BValue::Bytes(b) => b,
                            _ => return Err(anyhow!("path component is not bytes")),
                        };
                        let s = String::from_utf8(b.clone())
                            .map_err(|_| anyhow!("path component is not utf-8"))?;
                        validate_path_component(&s)?;
                        parsed_path.push(s);
                    }

                    let path_key = parsed_path.join("/");
                    if !seen_paths.insert(path_key) {
                        return Err(anyhow!("duplicate file path in torrent"));
                    }

                    parsed_files.push(FileInfo {
                        length: flen,
                        path: parsed_path,
                    });
                }

                let expected_pieces = total_length.div_ceil(piece_length);
                if num_pieces != expected_pieces {
                    return Err(anyhow!("pieces count mismatch for multi-file"));
                }

                Info::MultiFile(MultiFileInfo {
                    name,
                    piece_length,
                    pieces,
                    files: parsed_files,
                })
            }
            (Some(_), Some(_)) => return Err(anyhow!("both length and files are present")),
            (None, None) => return Err(anyhow!("neither length nor files are present")),
        };

        Ok(info)
    }
}

fn get_string(dict: &std::collections::BTreeMap<Vec<u8>, BValue>, key: &[u8]) -> Result<String> {
    let b = get_bytes(dict, key)?;
    String::from_utf8(b.clone())
        .map_err(|_| anyhow!("key {:?} is not valid utf-8", String::from_utf8_lossy(key)))
}

fn get_bytes<'a>(
    dict: &'a std::collections::BTreeMap<Vec<u8>, BValue>,
    key: &[u8],
) -> Result<&'a Vec<u8>> {
    match dict.get(key) {
        Some(BValue::Bytes(b)) => Ok(b),
        Some(_) => Err(anyhow!(
            "key {:?} is not bytes",
            String::from_utf8_lossy(key)
        )),
        None => Err(anyhow!("missing key {:?}", String::from_utf8_lossy(key))),
    }
}

fn get_int(dict: &std::collections::BTreeMap<Vec<u8>, BValue>, key: &[u8]) -> Result<i64> {
    match dict.get(key) {
        Some(BValue::Int(i)) => Ok(*i),
        Some(_) => Err(anyhow!("key {:?} is not int", String::from_utf8_lossy(key))),
        None => Err(anyhow!("missing key {:?}", String::from_utf8_lossy(key))),
    }
}

fn get_int_opt(dict: &std::collections::BTreeMap<Vec<u8>, BValue>, key: &[u8]) -> Option<i64> {
    match dict.get(key) {
        Some(BValue::Int(i)) => Some(*i),
        _ => None,
    }
}

fn get_list<'a>(
    dict: &'a std::collections::BTreeMap<Vec<u8>, BValue>,
    key: &[u8],
) -> Result<&'a Vec<BValue>> {
    match dict.get(key) {
        Some(BValue::List(l)) => Ok(l),
        Some(_) => Err(anyhow!(
            "key {:?} is not list",
            String::from_utf8_lossy(key)
        )),
        None => Err(anyhow!("missing key {:?}", String::from_utf8_lossy(key))),
    }
}

fn get_list_opt<'a>(
    dict: &'a std::collections::BTreeMap<Vec<u8>, BValue>,
    key: &[u8],
) -> Option<&'a Vec<BValue>> {
    match dict.get(key) {
        Some(BValue::List(l)) => Some(l),
        _ => None,
    }
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(anyhow!("name cannot be empty"));
    }
    validate_path_component(name)
}

fn validate_path_component(comp: &str) -> Result<()> {
    if comp.is_empty() {
        return Err(anyhow!("path component cannot be empty"));
    }
    if comp == "." || comp == ".." {
        return Err(anyhow!("path component cannot be . or .."));
    }
    if comp.contains(['/', '\\', '\0', ':']) {
        return Err(anyhow!("path component contains invalid characters"));
    }

    // Windows prefix check like "C:"
    let path = Path::new(comp);
    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            _ => {
                return Err(anyhow!(
                    "invalid path component type (absolute, prefix, etc)"
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bencode;

    #[test]
    fn verifies_sha1_digest() {
        let digest: [u8; 20] = Sha1::digest(b"abc").into();
        assert_eq!(
            crate::hex_lower(&digest),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
    }

    #[test]
    fn test_valid_single_file() {
        let mut info = std::collections::BTreeMap::new();
        info.insert(b"length".to_vec(), BValue::Int(20));
        info.insert(b"name".to_vec(), BValue::Bytes(b"test.txt".to_vec()));
        info.insert(b"piece length".to_vec(), BValue::Int(16384));
        info.insert(b"pieces".to_vec(), BValue::Bytes(vec![0; 20]));

        let mut meta = std::collections::BTreeMap::new();
        meta.insert(
            b"announce".to_vec(),
            BValue::Bytes(b"http://tracker".to_vec()),
        );
        meta.insert(b"info".to_vec(), BValue::Dict(info.clone()));

        let data = bencode::encode(&BValue::Dict(meta));
        let metainfo = Metainfo::from_bytes(&data).unwrap();

        assert_eq!(metainfo.announce.unwrap(), "http://tracker");
        match metainfo.info {
            Info::SingleFile(sf) => {
                assert_eq!(sf.name, "test.txt");
                assert_eq!(sf.length, 20);
                assert_eq!(sf.piece_length, 16384);
            }
            _ => panic!("expected single file"),
        }

        // Test Info::from_bytes
        let info_bytes = bencode::encode(&BValue::Dict(info));
        let mut hasher = Sha1::new();
        hasher.update(&info_bytes);
        let hash: [u8; 20] = hasher.finalize().into();

        let info_parsed = Info::from_bytes(&info_bytes, &hash).unwrap();
        match info_parsed {
            Info::SingleFile(sf) => {
                assert_eq!(sf.name, "test.txt");
            }
            _ => panic!("expected single file"),
        }
    }

    #[test]
    fn info_rejects_short_pieces_and_v2_only() {
        let mut short = b"d6:lengthi1e4:name1:x12:piece lengthi1000e6:pieces20:".to_vec();
        short.extend_from_slice(&[0; 20]);
        short.push(b'e');
        let v2 = b"d12:meta versioni2e4:name1:x12:piece lengthi16384ee";
        for (bytes, error) in [
            (
                short.as_slice(),
                "piece length must be between 16 KiB and 64 MiB",
            ),
            (v2.as_slice(), "BitTorrent v2-only torrents are unsupported"),
        ] {
            assert_eq!(
                Info::from_bytes(bytes, &Sha1::digest(bytes).into())
                    .unwrap_err()
                    .to_string(),
                error
            );
        }
    }

    #[test]
    fn test_invalid_path_components() {
        assert!(validate_path_component("..").is_err());
        assert!(validate_path_component(".").is_err());
        assert!(validate_path_component("a/b").is_err());
        assert!(validate_path_component("C:").is_err());
        assert!(validate_path_component("a:b").is_err());
        assert!(validate_path_component("").is_err());
        assert!(validate_path_component("a\\b").is_err());
        assert!(validate_path_component("a\0b").is_err());
        assert!(validate_path_component("valid").is_ok());
    }
}

#[cfg(test)]
mod fuzz_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn fuzz_metainfo_parser(data in any::<Vec<u8>>()) {
            let _ = Metainfo::from_bytes(&data);
        }
    }
}
