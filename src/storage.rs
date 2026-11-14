use anyhow::{Result, anyhow};
use std::fs::{File, OpenOptions};
use std::io;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;
use std::path::Path;

pub struct ChunkStorage {
    file: File,
    size: u64,
}

#[allow(dead_code)]
impl ChunkStorage {
    pub fn new<P: AsRef<Path>>(path: P, size: u64) -> Result<Self> {
        Self::open(path, size, true)
    }

    pub fn resume<P: AsRef<Path>>(path: P, size: u64) -> Result<Self> {
        Self::open(path, size, false)
    }

    fn open<P: AsRef<Path>>(path: P, size: u64, truncate: bool) -> Result<Self> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        if truncate {
            options.truncate(true);
        }

        let file = options.open(path)?;

        // Pre-allocate or stretch the file to the target size
        file.set_len(size)?;

        Ok(Self { file, size })
    }

    pub fn write_at(&self, offset: u64, data: &[u8]) -> Result<()> {
        let end = offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| anyhow!("Write offset overflow"))?;
        if end > self.size {
            return Err(anyhow!(
                "Write exceeds target size: end {} > size {}",
                end,
                self.size
            ));
        }

        write_all_at(&self.file, data, offset)?;
        Ok(())
    }

    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let end = offset
            .checked_add(buf.len() as u64)
            .ok_or_else(|| anyhow!("Read offset overflow"))?;
        if end > self.size {
            return Err(anyhow!(
                "Read exceeds target size: end {} > size {}",
                end,
                self.size
            ));
        }

        read_all_at(&self.file, buf, offset)?;
        Ok(())
    }

    pub fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }
}

fn write_all_at(file: &File, mut data: &[u8], mut offset: u64) -> io::Result<()> {
    while !data.is_empty() {
        let written = match write_once_at(file, data, offset) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };

        data = &data[written..];
        offset += written as u64;
    }

    Ok(())
}

#[allow(dead_code)]
fn read_all_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        let read = match read_once_at(file, buf, offset) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };

        let tmp = buf;
        buf = &mut tmp[read..];
        offset += read as u64;
    }

    Ok(())
}

#[cfg(unix)]
fn write_once_at(file: &File, data: &[u8], offset: u64) -> io::Result<usize> {
    file.write_at(data, offset)
}

#[cfg(unix)]
#[allow(dead_code)]
fn read_once_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    file.read_at(buf, offset)
}

#[cfg(windows)]
fn write_once_at(file: &File, data: &[u8], offset: u64) -> io::Result<usize> {
    file.seek_write(data, offset)
}

#[cfg(windows)]
#[allow(dead_code)]
fn read_once_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    file.seek_read(buf, offset)
}

#[cfg(test)]
mod tests {
    use super::ChunkStorage;
    use std::fs::OpenOptions;
    use std::io::Read;
    use std::time::Instant;

    #[test]
    fn writes_chunks_at_their_offsets() {
        let path = std::env::temp_dir().join(format!(
            "chorus-storage-{}-{}.tmp",
            std::process::id(),
            "offsets"
        ));
        let _ = std::fs::remove_file(&path);

        let storage = ChunkStorage::new(&path, 8).unwrap();
        storage.write_at(2, b"CD").unwrap();
        storage.write_at(6, b"GH").unwrap();
        drop(storage);

        let mut out = Vec::new();
        OpenOptions::new()
            .read(true)
            .open(&path)
            .unwrap()
            .read_to_end(&mut out)
            .unwrap();
        let _ = std::fs::remove_file(path);

        assert_eq!(out, b"\0\0CD\0\0GH");
    }

    #[test]
    fn resume_keeps_existing_bytes() {
        let path = std::env::temp_dir().join(format!(
            "chorus-storage-{}-{}.tmp",
            std::process::id(),
            "resume"
        ));
        let _ = std::fs::remove_file(&path);

        let storage = ChunkStorage::new(&path, 4).unwrap();
        storage.write_at(0, b"AB").unwrap();
        drop(storage);

        let storage = ChunkStorage::resume(&path, 4).unwrap();
        storage.write_at(2, b"CD").unwrap();
        drop(storage);

        let mut out = Vec::new();
        OpenOptions::new()
            .read(true)
            .open(&path)
            .unwrap()
            .read_to_end(&mut out)
            .unwrap();
        let _ = std::fs::remove_file(path);

        assert_eq!(out, b"ABCD");
    }

    #[test]
    fn rejects_writes_beyond_target_size() {
        let path = std::env::temp_dir().join(format!(
            "chorus-storage-{}-{}.tmp",
            std::process::id(),
            "bounds"
        ));
        let _ = std::fs::remove_file(&path);

        let storage = ChunkStorage::new(&path, 4).unwrap();
        assert!(storage.write_at(3, b"BC").is_err());
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    #[ignore = "manual I/O baseline: cargo test storage::tests::bench_offset_writes -- --ignored --nocapture"]
    fn bench_offset_writes() {
        let path = std::env::temp_dir().join(format!(
            "chorus-storage-{}-{}.tmp",
            std::process::id(),
            "bench"
        ));
        let _ = std::fs::remove_file(&path);

        let chunk = vec![42; 64 * 1024];
        let chunks = 1024_u64;
        let storage = ChunkStorage::new(&path, chunk.len() as u64 * chunks).unwrap();

        let started = Instant::now();
        for i in 0..chunks {
            storage.write_at(i * chunk.len() as u64, &chunk).unwrap();
        }
        let elapsed = started.elapsed();
        drop(storage);
        let _ = std::fs::remove_file(path);

        eprintln!(
            "wrote {} MiB in {:.2?}",
            chunk.len() as u64 * chunks / 1024 / 1024,
            elapsed
        );
    }
}
