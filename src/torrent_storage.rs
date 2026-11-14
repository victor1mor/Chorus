use crate::metainfo::Info;
use crate::storage::ChunkStorage;
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct FileSlice {
    pub file_index: usize,
    pub file_offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LayoutFile {
    pub length: u64,
    pub path_components: Vec<String>,
    pub offset_in_torrent: u64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Layout {
    pub files: Vec<LayoutFile>,
    pub total_length: u64,
    pub piece_length: u64,
}

#[allow(dead_code)]
impl Layout {
    pub fn from_info(info: &Info) -> Self {
        match info {
            Info::SingleFile(sf) => Layout {
                files: vec![LayoutFile {
                    length: sf.length as u64,
                    path_components: vec![sf.name.clone()],
                    offset_in_torrent: 0,
                }],
                total_length: sf.length as u64,
                piece_length: sf.piece_length as u64,
            },
            Info::MultiFile(mf) => {
                let mut files = Vec::new();
                let mut current_offset = 0;
                for f in &mf.files {
                    let mut comps = vec![mf.name.clone()];
                    comps.extend(f.path.clone());
                    files.push(LayoutFile {
                        length: f.length as u64,
                        path_components: comps,
                        offset_in_torrent: current_offset,
                    });
                    current_offset += f.length as u64;
                }
                Layout {
                    files,
                    total_length: current_offset,
                    piece_length: mf.piece_length as u64,
                }
            }
        }
    }

    pub fn map_slice(&self, offset: u64, mut length: u64) -> Result<Vec<FileSlice>> {
        if offset
            .checked_add(length)
            .ok_or_else(|| anyhow!("offset overflow"))?
            > self.total_length
        {
            return Err(anyhow!("Slice extends beyond total torrent length"));
        }
        if length == 0 {
            return Ok(Vec::new());
        }

        let mut slices = Vec::new();
        let mut current_offset = offset;

        let mut start_idx = 0;
        for (i, f) in self.files.iter().enumerate() {
            if f.length > 0 && current_offset < f.offset_in_torrent + f.length {
                start_idx = i;
                break;
            }
        }

        for i in start_idx..self.files.len() {
            let f = &self.files[i];
            if length == 0 {
                break;
            }
            if f.length == 0 {
                continue;
            }

            let file_offset = current_offset - f.offset_in_torrent;
            let available_in_file = f.length - file_offset;
            let slice_len = std::cmp::min(length, available_in_file);

            slices.push(FileSlice {
                file_index: i,
                file_offset,
                length: slice_len,
            });

            current_offset += slice_len;
            length -= slice_len;
        }

        Ok(slices)
    }
}

#[allow(dead_code)]
pub struct TorrentStorage {
    layout: Layout,
    files: Vec<ChunkStorage>,
}

#[allow(dead_code)]
impl TorrentStorage {
    pub fn new(root: &Path, info: &Info) -> Result<Self> {
        let layout = Layout::from_info(info);

        fs::create_dir_all(root)?;

        for f in &layout.files {
            let mut current = root.to_path_buf();
            for (i, comp) in f.path_components.iter().enumerate() {
                current.push(comp);
                match fs::symlink_metadata(&current) {
                    Ok(meta) if meta.is_symlink() => {
                        return Err(anyhow!("Symlink escape detected: {}", current.display()));
                    }
                    Ok(_) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        if i < f.path_components.len() - 1 {
                            fs::create_dir(&current)?;
                        }
                    }
                    Err(err) => return Err(err.into()),
                }
            }
        }

        let mut files = Vec::new();
        for f in &layout.files {
            let mut path = root.to_path_buf();
            for comp in &f.path_components {
                path.push(comp);
            }
            let storage = ChunkStorage::resume(&path, f.length)?;
            files.push(storage);
        }

        Ok(Self { layout, files })
    }

    pub fn read_piece(&self, piece_idx: usize, buf: &mut [u8]) -> Result<()> {
        let offset = (piece_idx as u64)
            .checked_mul(self.layout.piece_length)
            .unwrap();
        let slices = self.layout.map_slice(offset, buf.len() as u64)?;

        let mut buf_offset = 0;
        for slice in slices {
            let chunk = &mut buf[buf_offset..buf_offset + slice.length as usize];
            self.files[slice.file_index].read_at(slice.file_offset, chunk)?;
            buf_offset += slice.length as usize;
        }

        Ok(())
    }

    pub fn write_piece(&self, piece_idx: usize, data: &[u8]) -> Result<()> {
        let offset = (piece_idx as u64)
            .checked_mul(self.layout.piece_length)
            .unwrap();
        let slices = self.layout.map_slice(offset, data.len() as u64)?;

        let mut data_offset = 0;
        for slice in slices {
            let chunk = &data[data_offset..data_offset + slice.length as usize];
            self.files[slice.file_index].write_at(slice.file_offset, chunk)?;
            data_offset += slice.length as usize;
        }

        Ok(())
    }

    pub fn sync_all(&self) -> Result<()> {
        for f in &self.files {
            f.sync_all()?;
        }
        Ok(())
    }

    pub fn write_piece_and_validate(
        &self,
        piece_idx: usize,
        data: &[u8],
        expected_hash: &[u8; 20],
    ) -> Result<()> {
        let mut hasher = Sha1::new();
        hasher.update(data);
        let hash: [u8; 20] = hasher.finalize().into();

        if &hash != expected_hash {
            return Err(anyhow!("Piece hash mismatch"));
        }

        self.write_piece(piece_idx, data)?;
        self.sync_all()?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct TorrentPieceMap {
    pub bitfield: Vec<u8>,
    pub num_pieces: usize,
}

#[allow(dead_code)]
impl TorrentPieceMap {
    pub fn new(num_pieces: usize) -> Self {
        let len = num_pieces.div_ceil(8);
        Self {
            bitfield: vec![0; len],
            num_pieces,
        }
    }

    pub fn has_piece(&self, idx: usize) -> bool {
        idx < self.num_pieces
            && self
                .bitfield
                .get(idx / 8)
                .is_some_and(|byte| byte & (0x80 >> (idx % 8)) != 0)
    }

    pub fn matches_piece_count(&self, num_pieces: usize) -> bool {
        let remainder = num_pieces % 8;
        self.num_pieces == num_pieces
            && self.bitfield.len() == num_pieces.div_ceil(8)
            && (remainder == 0
                || self
                    .bitfield
                    .last()
                    .is_some_and(|byte| byte & (0xff >> remainder) == 0))
    }

    pub fn set_piece(&mut self, idx: usize) {
        if idx < self.num_pieces {
            self.bitfield[idx / 8] |= 1 << (7 - (idx % 8));
        }
    }

    pub fn clear_piece(&mut self, idx: usize) {
        if let Some(byte) = self.bitfield.get_mut(idx / 8) {
            *byte &= !(0x80 >> (idx % 8));
        }
    }

    fn paths(directory: &Path, id: &str) -> [PathBuf; 3] {
        let path = directory.join(format!("{id}.chorus.piecemap.json"));
        let temp = path.with_extension("piecemap.json.tmp");
        let backup = path.with_extension("piecemap.json.bak");
        [path, temp, backup]
    }

    pub fn exists(directory: &Path, id: &str) -> Result<bool> {
        for path in Self::paths(directory, id) {
            if path.try_exists()? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn save(&self, directory: &Path, id: &str) -> Result<PathBuf> {
        let [path, temp, backup] = Self::paths(directory, id);

        let mut file = std::fs::File::create(&temp)?;
        file.write_all(&serde_json::to_vec(self)?)?;
        file.sync_all()?;

        if path.exists() {
            fs::rename(&path, &backup)?;
            if let Err(err) = fs::rename(&temp, &path) {
                let _ = fs::rename(&backup, &path);
                let _ = fs::remove_file(&temp);
                return Err(err.into());
            }
            let _ = fs::remove_file(&backup);
        } else {
            fs::rename(&temp, &path)?;
        }
        Ok(path)
    }

    pub fn load(directory: &Path, id: &str) -> Result<Self> {
        let [path, temp, bak] = Self::paths(directory, id);
        let try_load = |p: &Path| -> Result<Self> {
            let data = fs::read(p)?;
            Ok(serde_json::from_slice(&data)?)
        };

        let candidates = [&path, &temp, &bak];
        let mut loaded = None;
        let mut first_err = None;

        for p in candidates {
            if p.exists() {
                match try_load(p) {
                    Ok(m) => {
                        loaded = Some((m, p.to_path_buf()));
                        break;
                    }
                    Err(e) => {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
            }
        }

        match loaded {
            Some((data, valid_path)) => {
                if valid_path != path {
                    let _ = fs::rename(&valid_path, &path);
                }
                if valid_path != temp && temp.exists() {
                    let _ = fs::remove_file(&temp);
                }
                if valid_path != bak && bak.exists() {
                    let _ = fs::remove_file(&bak);
                }
                Ok(data)
            }
            None => match first_err {
                Some(e) => Err(e),
                None => Err(anyhow::anyhow!("Piece map not found")),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metainfo::{MultiFileInfo, SingleFileInfo};

    #[test]
    fn test_layout_single_file() {
        let info = Info::SingleFile(SingleFileInfo {
            name: "test.txt".to_string(),
            piece_length: 5,
            pieces: vec![],
            length: 12,
        });

        let layout = Layout::from_info(&info);
        assert_eq!(layout.total_length, 12);
        assert_eq!(layout.files.len(), 1);

        let slices = layout.map_slice(4, 6).unwrap();
        assert_eq!(slices.len(), 1);
        assert_eq!(
            slices[0],
            FileSlice {
                file_index: 0,
                file_offset: 4,
                length: 6
            }
        );
    }

    #[test]
    fn test_layout_multi_file() {
        let info = Info::MultiFile(MultiFileInfo {
            name: "dir".to_string(),
            piece_length: 5,
            pieces: vec![],
            files: vec![
                crate::metainfo::FileInfo {
                    length: 3,
                    path: vec!["a.txt".to_string()],
                },
                crate::metainfo::FileInfo {
                    length: 0,
                    path: vec!["empty.txt".to_string()],
                },
                crate::metainfo::FileInfo {
                    length: 6,
                    path: vec!["b.txt".to_string()],
                },
            ],
        });

        let layout = Layout::from_info(&info);
        assert_eq!(layout.total_length, 9);
        assert_eq!(layout.files.len(), 3);

        // read crossing all files
        let slices = layout.map_slice(2, 4).unwrap();
        assert_eq!(slices.len(), 2);

        assert_eq!(
            slices[0],
            FileSlice {
                file_index: 0,
                file_offset: 2,
                length: 1
            }
        );
        // file_index 1 is empty, so it is skipped
        assert_eq!(
            slices[1],
            FileSlice {
                file_index: 2,
                file_offset: 0,
                length: 3
            }
        );
    }

    #[test]
    fn test_torrent_storage_symlink_rejection() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let dir = std::env::temp_dir().join(format!("chorus-symlink-{}", std::process::id()));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).unwrap();

            let target = dir.join("target");
            fs::create_dir(&target).unwrap();
            let link = dir.join("link");
            symlink(&target, &link).unwrap();

            let info = Info::SingleFile(SingleFileInfo {
                name: "link".to_string(),
                piece_length: 5,
                pieces: vec![],
                length: 12,
            });

            let res = TorrentStorage::new(&dir, &info);
            assert!(res.is_err());
            assert!(
                res.err()
                    .expect("TorrentStorage::new deveria falhar")
                    .to_string()
                    .contains("Symlink escape detected")
            );

            let _ = fs::remove_dir_all(&dir);
        }
    }

    #[test]
    #[cfg(unix)]
    fn test_torrent_storage_broken_symlink_rejection() {
        use crate::metainfo::FileInfo;
        use std::os::unix::fs::symlink;

        let dir = std::env::temp_dir().join(format!("chorus-dangling-link-{}", std::process::id()));
        let target = dir.join("outside");
        let output = dir.join("output");
        fs::create_dir_all(&output).unwrap();
        symlink(&target, output.join("link")).unwrap();

        let single = Info::SingleFile(SingleFileInfo {
            name: "link".into(),
            piece_length: 16384,
            pieces: vec![],
            length: 12,
        });
        let multi = Info::MultiFile(MultiFileInfo {
            name: "link".into(),
            piece_length: 16384,
            pieces: vec![],
            files: vec![FileInfo {
                length: 12,
                path: vec!["file.bin".into()],
            }],
        });

        for info in [single, multi] {
            let error = TorrentStorage::new(&output, &info)
                .err()
                .expect("broken symlink must be rejected");
            assert!(error.to_string().contains("Symlink escape detected"));
            assert!(!target.exists());
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_piece_crosses_multiple_files() {
        let dir = std::env::temp_dir().join(format!("chorus-cross-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let info = Info::MultiFile(MultiFileInfo {
            name: "cross".to_string(),
            piece_length: 5,
            pieces: vec![[0; 20], [0; 20]],
            files: vec![
                crate::metainfo::FileInfo {
                    length: 3,
                    path: vec!["a.txt".to_string()],
                },
                crate::metainfo::FileInfo {
                    length: 4,
                    path: vec!["b.txt".to_string()],
                },
            ],
        });

        let storage = TorrentStorage::new(&dir, &info).unwrap();

        let data = b"12345";
        storage.write_piece(0, data).unwrap();

        let mut buf = vec![0; 5];
        storage.read_piece(0, &mut buf).unwrap();
        assert_eq!(&buf, data);

        let a_data = fs::read(dir.join("cross").join("a.txt")).unwrap();
        assert_eq!(a_data, b"123");

        let b_data = fs::read(dir.join("cross").join("b.txt")).unwrap();
        assert_eq!(b_data, b"45\0\0");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_corrupt_piece_rejection() {
        let dir = std::env::temp_dir().join(format!("chorus-corrupt-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let mut expected_hash = [0; 20];
        // Hash of "hello"
        let mut hasher = Sha1::new();
        hasher.update(b"hello");
        expected_hash.copy_from_slice(&hasher.finalize());

        let info = Info::SingleFile(SingleFileInfo {
            name: "file.txt".to_string(),
            piece_length: 5,
            pieces: vec![expected_hash],
            length: 5,
        });

        let storage = TorrentStorage::new(&dir, &info).unwrap();

        let res = storage.write_piece_and_validate(0, b"world", &expected_hash);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("Piece hash mismatch"));

        let res = storage.write_piece_and_validate(0, b"hello", &expected_hash);
        assert!(res.is_ok());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_piecemap_save_and_load() {
        let dir = std::env::temp_dir().join(format!("chorus-piecemap-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let mut map = TorrentPieceMap::new(10);
        map.set_piece(2);
        map.set_piece(9);

        map.save(&dir, "test-job").unwrap();

        let loaded = TorrentPieceMap::load(&dir, "test-job").unwrap();
        assert_eq!(loaded.num_pieces, 10);
        assert!(!loaded.has_piece(1));
        assert!(loaded.has_piece(2));
        assert!(loaded.has_piece(9));
        assert!(!loaded.has_piece(10));

        let _ = fs::remove_dir_all(&dir);
    }
}
