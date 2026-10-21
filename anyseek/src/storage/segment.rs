//! Immutable, bounded payload blocks. A descriptor is prepared data, not a
//! publication witness: only an authenticated partition manifest may select it.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fs::{File, OpenOptions}, io::{self, Read, Seek, SeekFrom, Write}, path::Path, sync::Mutex};

const MAGIC: &[u8; 16] = b"ANYSEEK-PAYLOAD1";
const HEADER: u64 = 16;
fn invalid(message: &str) -> io::Error { io::Error::new(io::ErrorKind::InvalidData, message) }
fn hash(bytes: &[u8]) -> [u8; 32] { Sha256::digest(bytes).into() }

/// Exact bytes, not a record/body version: vector enrichment may retain that version.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PayloadRef {
    pub offset: u64,
    pub length: u64,
    pub sha256: [u8; 32],
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Descriptor {
    pub bytes: u64,
    pub sha256: [u8; 32],
    pub payloads: Vec<PayloadRef>,
}

pub(crate) struct Writer {
    file: File,
    limit: u64,
    end: u64,
    max_payloads: usize,
    poisoned: bool,
    payloads: Vec<PayloadRef>,
}
impl Writer {
    /// Call in a private build directory. Never overwrite any existing object.
    pub fn create(path: &Path, limit: u64, max_payloads: usize) -> io::Result<Self> {
        if limit < HEADER { return Err(invalid("segment budget smaller than header")); }
        let mut file=OpenOptions::new().read(true).write(true).create_new(true).open(path)?;
        file.write_all(MAGIC)?;
        Ok(Self {file,limit,end:HEADER,max_payloads,poisoned:false,payloads:Vec::new()})
    }
    /// Deduplicate only byte-identical input after a full comparison; a digest
    /// collision never aliases different requests. Does not retain body copies.
    pub fn intern(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.poisoned { return Err(invalid("segment writer requires discard")); }
        let digest=hash(bytes);
        for (position,old) in self.payloads.iter().enumerate() {
            if old.length==bytes.len() as u64 && old.sha256==digest {
                let comparison = (|| -> io::Result<bool> {
                    self.file.seek(SeekFrom::Start(old.offset))?;
                    let mut left=bytes;
                    let mut buffer=[0u8;8192];
                    while !left.is_empty() {
                        let n=left.len().min(buffer.len());
                        self.file.read_exact(&mut buffer[..n])?;
                        if buffer[..n]!=left[..n] {return Ok(false);}
                        left=&left[n..];
                    }
                    Ok(true)
                })();
                let equal=match comparison {Ok(equal)=>equal,Err(error)=>{self.poisoned=true;return Err(error);}};
                if equal {return Ok(position);}
            }
        }
        if self.payloads.len() >= self.max_payloads {return Err(invalid("segment payload count exceeded"));}
        let end=self.end.checked_add(bytes.len() as u64).ok_or_else(||invalid("segment length overflow"))?;
        if end>self.limit {return Err(invalid("segment payload budget exceeded"));}
        // A failed append may have written a prefix. Never permit a later
        // append/finish to turn that uncertain partial object into a candidate.
        self.poisoned=true;
        self.file.seek(SeekFrom::Start(self.end))?;
        self.file.write_all(bytes)?;
        self.poisoned=false;
        let position=self.payloads.len();
        self.payloads.push(PayloadRef{offset:self.end,length:bytes.len() as u64,sha256:digest});
        self.end=end;
        Ok(position)
    }
    pub fn finish(mut self) -> io::Result<Descriptor> {
        if self.poisoned { return Err(invalid("segment writer requires discard")); }
        if self.file.metadata()?.len()!=self.end {return Err(invalid("incomplete segment write"));}
        self.file.sync_all()?;
        self.file.seek(SeekFrom::Start(0))?;
        let mut digest=Sha256::new();
        let mut buffer=[0u8;8192];
        loop {let n=self.file.read(&mut buffer)?;if n==0 {break;}digest.update(&buffer[..n]);}
        Ok(Descriptor{bytes:self.end,sha256:digest.finalize().into(),payloads:self.payloads})
    }
}

pub(crate) struct Reader {file:Mutex<File>, descriptor:Descriptor}
impl Reader {
    /// Validate the authenticated descriptor before allocating a body. The
    /// caller owns path/lineage authorization; this reader grants none.
    pub fn open(path:&Path,descriptor:Descriptor,max_bytes:u64,max_payloads:usize)->io::Result<Self> {
        if descriptor.bytes<HEADER || descriptor.bytes>max_bytes || descriptor.payloads.len()>max_payloads {
            return Err(invalid("segment descriptor budget exceeded"));
        }
        if std::fs::symlink_metadata(path)?.file_type().is_symlink() {return Err(invalid("segment symlink rejected"));}
        let mut file=File::open(path)?;
        if !file.metadata()?.is_file() || file.metadata()?.len()!=descriptor.bytes {return Err(invalid("segment length mismatch"));}
        let mut end=HEADER;
        for p in &descriptor.payloads {
            if p.offset!=end {return Err(invalid("non-contiguous payload descriptor"));}
            end=end.checked_add(p.length).ok_or_else(||invalid("payload range overflow"))?;
            if end>descriptor.bytes {return Err(invalid("payload outside segment"));}
        }
        if end!=descriptor.bytes {return Err(invalid("unreferenced segment suffix"));}
        let mut magic=[0u8;16];file.read_exact(&mut magic)?;
        if &magic!=MAGIC {return Err(invalid("unsupported payload segment"));}
        file.seek(SeekFrom::Start(0))?;
        let mut digest=Sha256::new();let mut buffer=[0u8;8192];
        loop {let n=file.read(&mut buffer)?;if n==0 {break;}digest.update(&buffer[..n]);}
        let actual:[u8;32]=digest.finalize().into();
        if actual!=descriptor.sha256 {return Err(invalid("segment checksum mismatch"));}
        Ok(Self {file:Mutex::new(file),descriptor})
    }
    pub fn read(&self,position:usize,max_body_bytes:usize)->io::Result<Vec<u8>> {
        let p=self.descriptor.payloads.get(position).ok_or_else(||invalid("payload identity outside segment"))?;
        let length=usize::try_from(p.length).map_err(|_|invalid("payload length overflow"))?;
        if length>max_body_bytes {return Err(invalid("payload read budget exceeded"));}
        let mut bytes=Vec::new();bytes.try_reserve_exact(length).map_err(io::Error::other)?;bytes.resize(length,0);
        let mut file=self.file.lock().map_err(|_|invalid("payload reader poisoned"))?;
        file.seek(SeekFrom::Start(p.offset))?;file.read_exact(&mut bytes)?;
        if hash(&bytes)!=p.sha256 {return Err(invalid("payload checksum mismatch"));}
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn path()->std::path::PathBuf {std::env::temp_dir().join(format!("anyseek-payload-{:032x}",rand::random::<u128>()))}
    #[test]
    fn bounded_payload_roundtrip_and_corruption() {
        let path=path();
        let mut writer=Writer::create(&path,128,2).unwrap();
        assert_eq!(writer.intern(b"original").unwrap(),0);
        assert_eq!(writer.intern(b"original").unwrap(),0);
        assert_eq!(writer.intern(b"enriched").unwrap(),1);
        assert!(writer.intern(&[0;128]).is_err());
        let descriptor=writer.finish().unwrap();
        assert_eq!(descriptor.bytes,32);
        let reader=Reader::open(&path,descriptor.clone(),128,2).unwrap();
        assert_eq!(reader.read(0,8).unwrap(),b"original");
        assert_eq!(reader.read(1,8).unwrap(),b"enriched");
        assert!(reader.read(0,7).is_err());assert!(reader.read(2,8).is_err());
        drop(reader);
        assert!(Writer::create(&path,128,2).is_err());
        let mut bad=descriptor.clone();bad.payloads[0].length=u64::MAX;
        assert!(Reader::open(&path,bad,128,2).is_err());
        let mut file=OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(HEADER)).unwrap();file.write_all(b"X").unwrap();drop(file);
        assert!(Reader::open(&path,descriptor,128,2).is_err());
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn failed_payload_comparison_requires_discard() {
        let path=path();let mut writer=Writer::create(&path,128,2).unwrap();
        writer.intern(b"original").unwrap();
        // Inject a short read after an earlier successful append. A caller
        // ignoring the error cannot later certify this damaged prepared object.
        writer.file.set_len(HEADER).unwrap();
        assert!(writer.intern(b"original").is_err());
        assert!(writer.intern(b"another").is_err());
        assert!(writer.finish().is_err());
        std::fs::remove_file(path).unwrap();
    }

}
