use anyhow::{Result, anyhow, ensure};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MagnetLink {
    pub info_hash: [u8; 20],
    pub name: Option<String>,
    pub trackers: Vec<String>,
}

impl MagnetLink {
    pub fn parse(url: &str) -> Result<Self> {
        ensure!(url.len() <= 16 * 1024, "Magnet link exceeds 16 KiB");
        if !url.starts_with("magnet:?") {
            return Err(anyhow!("Not a magnet link"));
        }

        let query = &url["magnet:?".len()..];
        let mut info_hash = None;
        let mut name = None;
        let mut trackers = Vec::new();

        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }

            let mut split = pair.splitn(2, '=');
            let key = split.next().unwrap();
            let value = split.next().unwrap_or("");

            match key {
                "xt" if let Some(hash_str) = value.strip_prefix("urn:btih:") => {
                    info_hash = Some(decode_hash(hash_str)?);
                }
                "dn" => {
                    name = Some(url_decode(value));
                }
                "tr" if trackers.len() < 10 => {
                    trackers.push(url_decode(value));
                }
                _ => {}
            }
        }

        let info_hash = info_hash.ok_or_else(|| anyhow!("Missing or invalid xt=urn:btih:"))?;

        Ok(Self {
            info_hash,
            name,
            trackers,
        })
    }
}

fn decode_hash(hash_str: &str) -> Result<[u8; 20]> {
    let hash_str = hash_str.trim();
    if hash_str.len() == 40 {
        // Hexadecimal
        ensure!(
            hash_str.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "Invalid hex character in info hash"
        );
        let mut bytes = [0u8; 20];
        for i in 0..20 {
            let slice = &hash_str[i * 2..i * 2 + 2];
            bytes[i] = u8::from_str_radix(slice, 16)
                .map_err(|_| anyhow!("Invalid hex character in info hash"))?;
        }
        Ok(bytes)
    } else if hash_str.len() == 32 {
        // Base32
        decode_base32(hash_str)
    } else {
        Err(anyhow!(
            "Info hash must be 40 (hex) or 32 (base32) characters long"
        ))
    }
}

fn decode_base32(encoded: &str) -> Result<[u8; 20]> {
    let mut bytes = [0u8; 20];
    let mut buffer = 0u32;
    let mut bits_left = 0;
    let mut byte_idx = 0;

    for ch in encoded.chars() {
        let val = match ch {
            'A'..='Z' => ch as u32 - 'A' as u32,
            'a'..='z' => ch as u32 - 'a' as u32,
            '2'..='7' => ch as u32 - '2' as u32 + 26,
            _ => return Err(anyhow!("Invalid base32 character: {}", ch)),
        };

        buffer = (buffer << 5) | val;
        bits_left += 5;

        if bits_left >= 8 {
            if byte_idx >= 20 {
                return Err(anyhow!("Base32 string encodes too many bytes"));
            }
            bits_left -= 8;
            bytes[byte_idx] = (buffer >> bits_left) as u8;
            byte_idx += 1;
        }
    }

    if byte_idx != 20 {
        return Err(anyhow!("Base32 string does not encode exactly 20 bytes"));
    }

    Ok(bytes)
}

fn url_decode(encoded: &str) -> String {
    let mut bytes = Vec::with_capacity(encoded.len());
    let mut chars = encoded.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '%' {
            if let (Some(h1), Some(h2)) = (chars.next(), chars.next()) {
                let hex_str = format!("{}{}", h1, h2);
                if let Ok(b) = u8::from_str_radix(&hex_str, 16) {
                    bytes.push(b);
                } else {
                    bytes.push(b'%');
                    bytes.extend(h1.to_string().into_bytes());
                    bytes.extend(h2.to_string().into_bytes());
                }
            } else {
                bytes.push(b'%');
            }
        } else if c == '+' {
            bytes.push(b' ');
        } else {
            bytes.extend(c.to_string().into_bytes());
        }
    }

    String::from_utf8_lossy(&bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_magnet_hex() {
        let url = "magnet:?xt=urn:btih:3fa25039fdd05dc34eb7b63fde4f181f08bf6d85&dn=archlinux-2024.01.01-x86_64.iso&tr=udp%3A%2F%2Ftracker.archlinux.org%3A6969&tr=http%3A%2F%2Ftracker.archlinux.org%3A6969%2Fannounce";
        let magnet = MagnetLink::parse(url).unwrap();

        let expected_hash = [
            0x3f, 0xa2, 0x50, 0x39, 0xfd, 0xd0, 0x5d, 0xc3, 0x4e, 0xb7, 0xb6, 0x3f, 0xde, 0x4f,
            0x18, 0x1f, 0x08, 0xbf, 0x6d, 0x85,
        ];
        assert_eq!(magnet.info_hash, expected_hash);
        assert_eq!(magnet.name.unwrap(), "archlinux-2024.01.01-x86_64.iso");
        assert_eq!(
            magnet.trackers,
            vec![
                "udp://tracker.archlinux.org:6969",
                "http://tracker.archlinux.org:6969/announce"
            ]
        );
    }

    #[test]
    fn test_parse_magnet_base32() {
        let url = "magnet:?xt=urn:btih:H6RFAOP52BO4GTVXWY754TYYD4EL63MF";
        let magnet = MagnetLink::parse(url).unwrap();
        let expected_hash = [
            0x3f, 0xa2, 0x50, 0x39, 0xfd, 0xd0, 0x5d, 0xc3, 0x4e, 0xb7, 0xb6, 0x3f, 0xde, 0x4f,
            0x18, 0x1f, 0x08, 0xbf, 0x6d, 0x85,
        ];
        assert_eq!(magnet.info_hash, expected_hash);
    }

    #[test]
    fn limits_link_length_and_tracker_count() {
        let base = "magnet:?xt=urn:btih:H6RFAOP52BO4GTVXWY754TYYD4EL63MF";
        let link = format!("{base}&dn={}", "x".repeat(16 * 1024 - base.len() - 4));
        assert_eq!(link.len(), 16 * 1024);
        assert!(MagnetLink::parse(&link).is_ok());
        assert!(MagnetLink::parse(&(link + "x")).is_err());

        let trackers: Vec<_> = (0..12).map(|i| format!("http://localhost/{i}")).collect();
        let link = format!("{base}&tr={}", trackers.join("&tr="));
        assert_eq!(MagnetLink::parse(&link).unwrap().trackers, trackers[..10]);
    }
}
