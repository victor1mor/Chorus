use crate::rede::{ResolvedorProtegido, host_proibido};
use anyhow::{Result, anyhow};
use reqwest::header::{
    ACCEPT_ENCODING, ACCEPT_RANGES, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, ETAG,
    IF_RANGE, LAST_MODIFIED, RANGE,
};
use reqwest::{Client, StatusCode};
use std::time::Duration;

pub struct Metadata {
    pub content_length: u64,
    pub length_known: bool,
    pub supports_ranges: bool,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

#[derive(Clone)]
pub struct HttpFetcher {
    client: Client,
}

impl HttpFetcher {
    pub fn new(
        connect_timeout: Option<Duration>,
        read_timeout: Option<Duration>,
        timeout: Option<Duration>,
        rede_protegida: bool,
    ) -> Result<Self> {
        let mut builder = Client::builder()
            .no_gzip()
            .no_brotli()
            .no_zstd()
            .no_deflate()
            .redirect(reqwest::redirect::Policy::custom(move |attempt| {
                if rede_protegida && host_proibido(attempt.url()) {
                    return attempt.error("redirect to a private or local address");
                }
                if attempt.previous().len() >= 10 {
                    return attempt.error("too many redirects");
                }
                let next_url = attempt.url().as_str();
                for prev in attempt.previous() {
                    if prev.as_str() == next_url {
                        return attempt.error("redirect cycle detected");
                    }
                }
                attempt.follow()
            }));

        if rede_protegida {
            // Proxies podem resolver o destino fora do resolvedor protegido.
            builder = builder.no_proxy().dns_resolver(ResolvedorProtegido);
        }
        if let Some(connect_timeout) = connect_timeout {
            builder = builder.connect_timeout(connect_timeout);
        }
        if let Some(read_timeout) = read_timeout {
            builder = builder.read_timeout(read_timeout);
        }
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }

        Ok(Self {
            client: builder.build()?,
        })
    }

    pub async fn get_metadata(&self, url: &str) -> Result<Metadata> {
        let resp = self
            .client
            .head(url)
            .header(ACCEPT_ENCODING, "identity")
            .send()
            .await?;

        if !resp.status().is_success() {
            if let Some(metadata) = self.probe_range(url).await? {
                return Ok(metadata);
            }
            if resp.status() == StatusCode::METHOD_NOT_ALLOWED {
                return Ok(metadata_from_headers(
                    &reqwest::header::HeaderMap::new(),
                    false,
                ));
            }
            return Err(anyhow!(
                "Failed to fetch metadata, status: {}",
                resp.status()
            ));
        }

        ensure_identity_encoding(resp.headers(), "metadata")?;
        let metadata = metadata_from_headers(resp.headers(), false);
        if metadata.content_length > 0 && metadata.supports_ranges {
            return Ok(metadata);
        }

        if let Some(probed) = self.probe_range(url).await? {
            return Ok(Metadata {
                content_length: probed.content_length,
                length_known: true,
                supports_ranges: true,
                etag: probed.etag.or(metadata.etag),
                last_modified: probed.last_modified.or(metadata.last_modified),
            });
        }

        Ok(metadata)
    }

    async fn probe_range(&self, url: &str) -> Result<Option<Metadata>> {
        let resp = self
            .client
            .get(url)
            .header(RANGE, "bytes=0-0")
            .header(ACCEPT_ENCODING, "identity")
            .send()
            .await?;

        if resp.status() != StatusCode::PARTIAL_CONTENT {
            return Ok(None);
        }

        let headers = resp.headers();
        ensure_identity_encoding(headers, "range probe")?;
        let Some((0, 0, total)) = headers
            .get(CONTENT_RANGE)
            .and_then(|val| val.to_str().ok())
            .and_then(content_range_parts)
        else {
            return Ok(None);
        };

        Ok(Some(Metadata {
            content_length: total,
            length_known: true,
            supports_ranges: true,
            etag: header_string(headers.get(ETAG)),
            last_modified: header_string(headers.get(LAST_MODIFIED)),
        }))
    }

    pub async fn fetch_full(&self, url: &str) -> Result<reqwest::Response> {
        let resp = self
            .client
            .get(url)
            .header(ACCEPT_ENCODING, "identity")
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!("Failed to fetch file, status: {}", resp.status()));
        }
        ensure_identity_encoding(resp.headers(), "file")?;

        Ok(resp)
    }

    pub async fn fetch_chunk(
        &self,
        url: &str,
        start: u64,
        end: u64,
        total: u64,
        if_range: Option<&str>,
    ) -> Result<reqwest::Response> {
        let range_header = format!("bytes={}-{}", start, end);
        let mut request = self
            .client
            .get(url)
            .header(RANGE, range_header)
            .header(ACCEPT_ENCODING, "identity");
        if let Some(if_range) = if_range {
            request = request.header(IF_RANGE, if_range);
        }

        let resp = request.send().await?;

        if resp.status() != StatusCode::PARTIAL_CONTENT {
            return Err(anyhow!(
                "Server ignored range {}-{}; expected 206, got {}",
                start,
                end,
                resp.status()
            ));
        }
        ensure_identity_encoding(resp.headers(), "chunk")?;

        let content_range = resp
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|val| val.to_str().ok())
            .ok_or_else(|| anyhow!("Missing Content-Range for chunk {}-{}", start, end))?;

        if content_range_parts(content_range) != Some((start, end, total)) {
            return Err(anyhow!(
                "Unexpected Content-Range for chunk {}-{}: {}",
                start,
                end,
                content_range
            ));
        }

        Ok(resp)
    }
}

fn metadata_from_headers(headers: &reqwest::header::HeaderMap, supports_ranges: bool) -> Metadata {
    let content_length = content_length_from_headers(headers);

    let supports_ranges = supports_ranges
        || headers
            .get(ACCEPT_RANGES)
            .and_then(|val| val.to_str().ok())
            .map(|val| val.eq_ignore_ascii_case("bytes"))
            .unwrap_or(false);

    Metadata {
        content_length: content_length.unwrap_or(0),
        length_known: content_length.is_some(),
        supports_ranges,
        etag: header_string(headers.get(ETAG)),
        last_modified: header_string(headers.get(LAST_MODIFIED)),
    }
}

pub(crate) fn content_length_from_headers(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

fn header_string(value: Option<&reqwest::header::HeaderValue>) -> Option<String> {
    value
        .and_then(|val| val.to_str().ok())
        .map(|val| val.to_string())
}

fn ensure_identity_encoding(headers: &reqwest::header::HeaderMap, context: &str) -> Result<()> {
    let Some(value) = headers
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(());
    };

    if value.trim().eq_ignore_ascii_case("identity") {
        return Ok(());
    }

    Err(anyhow!(
        "Unsupported Content-Encoding for {}: {}",
        context,
        value
    ))
}

#[cfg(test)]
fn content_range_matches(value: &str, start: u64, end: u64) -> bool {
    content_range_parts(value).map(|(got_start, got_end, _)| (got_start, got_end))
        == Some((start, end))
}

fn content_range_parts(value: &str) -> Option<(u64, u64, u64)> {
    let (range, total) = value.strip_prefix("bytes ")?.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    Some((
        start.parse::<u64>().ok()?,
        end.parse::<u64>().ok()?,
        total.parse::<u64>().ok()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::{content_range_matches, content_range_parts, ensure_identity_encoding};
    use reqwest::header::{CONTENT_ENCODING, HeaderMap, HeaderValue};

    #[test]
    fn validates_content_range() {
        assert!(content_range_matches("bytes 10-19/100", 10, 19));
        assert!(!content_range_matches("bytes 0-19/100", 10, 19));
        assert!(!content_range_matches("items 10-19/100", 10, 19));
        assert_eq!(content_range_parts("bytes 10-19/100"), Some((10, 19, 100)));
        assert_eq!(content_range_parts("bytes 10-19/*"), None);
    }

    #[test]
    fn rejects_encoded_responses() {
        let mut headers = HeaderMap::new();
        assert!(ensure_identity_encoding(&headers, "test").is_ok());

        headers.insert(CONTENT_ENCODING, HeaderValue::from_static("identity"));
        assert!(ensure_identity_encoding(&headers, "test").is_ok());

        headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        assert!(ensure_identity_encoding(&headers, "test").is_err());
    }

    #[tokio::test]
    async fn detects_redirect_cycles() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut stream = stream;
                let mut buf = [0; 1024];
                let _ = stream.read(&mut buf);

                let response = format!(
                    "HTTP/1.1 302 Found\r\nLocation: http://{}/\r\nContent-Length: 0\r\n\r\n",
                    addr
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });

        let fetcher = super::HttpFetcher::new(None, None, None, false).unwrap();
        let url = format!("http://{}/", addr);
        let err = fetcher.fetch_full(&url).await.unwrap_err();
        let err_str = format!("{:?}", err);
        assert!(err_str.contains("redirect cycle") || err_str.contains("too many"));
    }

    #[tokio::test]
    async fn limits_redirects_to_10() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        std::thread::spawn(move || {
            let mut count = 0;
            for stream in listener.incoming().flatten() {
                count += 1;
                let mut stream = stream;
                let mut buf = [0; 1024];
                let _ = stream.read(&mut buf);

                let response = format!(
                    "HTTP/1.1 302 Found\r\nLocation: http://{}/{}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    addr, count
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });

        let fetcher = super::HttpFetcher::new(None, None, None, false).unwrap();
        let url = format!("http://{}/start", addr);
        let err = fetcher.fetch_full(&url).await.unwrap_err();
        let err_str = format!("{:?}", err);
        assert!(err_str.contains("too many redirects"), "{err_str}");
    }

    #[tokio::test]
    async fn protected_fetcher_rejects_redirect_to_loopback() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            assert_ne!(stream.read(&mut request).await.unwrap(), 0);
            stream
                .write_all(b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:0/x\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
        });

        let fetcher =
            super::HttpFetcher::new(None, None, Some(std::time::Duration::from_secs(5)), true)
                .unwrap();
        let err = fetcher
            .fetch_full(&format!("http://{addr}/"))
            .await
            .unwrap_err();
        assert!(format!("{err:?}").contains("redirect to a private or local address"));
    }
}
