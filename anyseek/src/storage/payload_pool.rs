//! Immutable payload objects shared by successive authenticated state anchors.
//! This prepares reference closures; only PartitionJournal can publish one.
use super::segment::{Descriptor, Reader, Writer};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::{Path, PathBuf},
};

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn valid_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn path(root: &Path, id: &str) -> io::Result<PathBuf> {
    if !valid_id(id) {
        return Err(invalid("invalid payload object identity"));
    }
    Ok(root.join(format!("payload-{id}")))
}
/// Physical identity survives another state anchor or retrieval compaction.
/// Neither the object identity nor its hash grants authorization.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Reference {
    pub object: String,
    pub position: usize,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Closure {
    pub objects: BTreeMap<String, Descriptor>,
}
#[derive(Clone, Copy)]
pub(crate) struct Budget {
    pub bytes: u64,
    pub objects: usize,
    pub payloads: usize,
    pub body_bytes: usize,
}

pub(crate) struct PoolReader {
    closure: Closure,
    readers: BTreeMap<String, Reader>,
    budget: Budget,
}
impl PoolReader {
    pub fn open(root: &Path, closure: Closure, budget: Budget) -> io::Result<Self> {
        let kind = std::fs::symlink_metadata(root)?.file_type();
        if !kind.is_dir() || kind.is_symlink() {
            return Err(invalid("invalid payload object root"));
        }
        if closure.objects.len() > budget.objects {
            return Err(invalid("payload object count exceeded"));
        }
        let mut bytes = 0u64;
        let mut payloads = 0usize;
        // Check the complete metadata budget before opening any object.
        for (id, descriptor) in &closure.objects {
            path(root, id)?;
            bytes = bytes
                .checked_add(descriptor.bytes)
                .filter(|n| *n <= budget.bytes)
                .ok_or_else(|| invalid("payload closure byte budget exceeded"))?;
            payloads = payloads
                .checked_add(descriptor.payloads.len())
                .filter(|n| *n <= budget.payloads)
                .ok_or_else(|| invalid("payload closure count exceeded"))?;
        }
        let mut readers = BTreeMap::new();
        for (id, descriptor) in &closure.objects {
            readers.insert(
                id.clone(),
                Reader::open(
                    &path(root, id)?,
                    descriptor.clone(),
                    budget.bytes,
                    budget.payloads,
                )?,
            );
        }
        Ok(Self {
            closure,
            readers,
            budget,
        })
    }
    pub fn read(&self, reference: &Reference) -> io::Result<Vec<u8>> {
        self.read_limited(reference, self.budget.body_bytes)
    }
    pub fn read_limited(
        &self,
        reference: &Reference,
        max_body_bytes: usize,
    ) -> io::Result<Vec<u8>> {
        self.readers
            .get(&reference.object)
            .ok_or_else(|| invalid("payload reference outside authenticated closure"))?
            .read(
                reference.position,
                self.budget.body_bytes.min(max_body_bytes),
            )
    }
}

/// A bounded builder reuses exact existing bytes. It never rewrites, retires or
/// deletes prior objects, and never infers authority from directory contents.
pub(crate) struct PoolBuilder {
    root: PathBuf,
    prior: PoolReader,
    used: BTreeSet<String>,
    id: String,
    writer: Option<Writer>,
    remaining_bytes: u64,
    remaining_payloads: usize,
    poisoned: bool,
}
impl PoolBuilder {
    pub fn begin(root: &Path, prior: Closure, budget: Budget, id: String) -> io::Result<Self> {
        path(root, &id)?;
        if prior.objects.contains_key(&id) {
            return Err(invalid("payload object identity already selected"));
        }
        let prior = PoolReader::open(root, prior, budget)?;
        let bytes = prior.closure.objects.values().map(|d| d.bytes).sum::<u64>();
        let payloads = prior
            .closure
            .objects
            .values()
            .map(|d| d.payloads.len())
            .sum::<usize>();
        Ok(Self {
            root: root.into(),
            prior,
            used: BTreeSet::new(),
            id,
            writer: None,
            remaining_bytes: budget.bytes - bytes,
            remaining_payloads: budget.payloads - payloads,
            poisoned: false,
        })
    }
    pub fn intern(&mut self, bytes: &[u8]) -> io::Result<Reference> {
        if self.poisoned {
            return Err(invalid("payload pool requires discard"));
        }
        if bytes.len() > self.prior.budget.body_bytes {
            return Err(invalid("payload body budget exceeded"));
        }
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        for (id, descriptor) in &self.prior.closure.objects {
            for (position, payload) in descriptor.payloads.iter().enumerate() {
                if payload.length != bytes.len() as u64 || payload.sha256 != digest {
                    continue;
                }
                let reference = Reference {
                    object: id.clone(),
                    position,
                };
                let old = match self.prior.read(&reference) {
                    Ok(old) => old,
                    Err(error) => {
                        self.poisoned = true;
                        return Err(error);
                    }
                };
                if old == bytes {
                    self.used.insert(id.clone());
                    return Ok(reference);
                }
            }
        }
        if self.writer.is_none() {
            if self.prior.closure.objects.len() >= self.prior.budget.objects {
                return Err(invalid("payload build object budget exceeded"));
            }
            // Includes prior objects even if some may later become unreferenced:
            // they cannot be reclaimed to make room before manifest publication.
            match path(&self.root,&self.id)?.try_exists(){
                Ok(false)=>{},
                Ok(true)=>{self.poisoned=true;return Err(invalid("payload object name already exists"));},
                Err(error)=>{self.poisoned=true;return Err(error);}
            }
            self.writer = Some(
                match Writer::create(
                    &self.root.join(format!(".payload-prepare-{}",self.id)),
                    self.remaining_bytes,
                    self.remaining_payloads,
                ) {
                    Ok(writer) => writer,
                    Err(error) => {
                        self.poisoned = true;
                        return Err(error);
                    }
                },
            );
        }
        match self.writer.as_mut().unwrap().intern(bytes) {
            Ok(position) => Ok(Reference {
                object: self.id.clone(),
                position,
            }),
            Err(error) => {
                self.poisoned = true;
                Err(error)
            }
        }
    }
    pub fn finish(self) -> io::Result<Closure> {
        if self.poisoned {
            return Err(invalid("payload pool requires discard"));
        }
        let mut objects = self
            .prior
            .closure
            .objects
            .into_iter()
            .filter(|(id, _)| self.used.contains(id))
            .collect::<BTreeMap<_, _>>();
        if let Some(writer) = self.writer {
            let descriptor=writer.finish()?;
            super::platform::publish_file(&self.root.join(format!(".payload-prepare-{}",self.id)),&path(&self.root,&self.id)?)
                .map_err(|error|match error {super::platform::PublishError::BeforeRename(e)|super::platform::PublishError::RenameAttempted(e)=>e})?;
            objects.insert(self.id,descriptor);
        }
        Ok(Closure { objects })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn root() -> PathBuf {
        let p = std::env::temp_dir().join(format!("anyseek-pool-{:032x}", rand::random::<u128>()));
        std::fs::create_dir(&p).unwrap();
        p
    }
    fn budget() -> Budget {
        Budget {
            bytes: 512,
            objects: 4,
            payloads: 8,
            body_bytes: 64,
        }
    }
    #[test]
    fn failed_new_object_create_cannot_certify_prior_references() {
        let root = root();
        let mut first =
            PoolBuilder::begin(&root, Closure::default(), budget(), format!("{:032x}", 1)).unwrap();
        first.intern(b"original").unwrap();
        let closure = first.finish().unwrap();
        let id = format!("{:032x}", 2);
        let mut next = PoolBuilder::begin(&root, closure, budget(), id.clone()).unwrap();
        next.intern(b"original").unwrap();
        // An unselected file is never overwritten or adopted as a new object.
        std::fs::write(path(&root, &id).unwrap(), b"unselected").unwrap();
        assert!(next.intern(b"new").is_err());
        assert!(next.intern(b"original").is_err());
        assert!(next.finish().is_err());
        assert_eq!(
            std::fs::read(path(&root, &id).unwrap()).unwrap(),
            b"unselected"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_prior_object_comparison_poisons_candidate() {
        use std::io::Write;
        let root = root();
        let mut first =
            PoolBuilder::begin(&root, Closure::default(), budget(), format!("{:032x}", 1)).unwrap();
        let reference = first.intern(b"original").unwrap();
        let closure = first.finish().unwrap();
        let mut next = PoolBuilder::begin(&root, closure, budget(), format!("{:032x}", 2)).unwrap();
        // Corrupt only this test's immutable object after its initial validation.
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(path(&root, &reference.object).unwrap())
            .unwrap();
        file.set_len(0).unwrap();
        file.write_all(b"damaged").unwrap();
        drop(file);
        assert!(next.intern(b"original").is_err());
        assert!(next.intern(b"new").is_err());
        assert!(next.finish().is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn successive_anchors_reuse_exact_objects_and_reject_unselected_references() {
        let root = root();
        let mut first =
            PoolBuilder::begin(&root, Closure::default(), budget(), format!("{:032x}", 1)).unwrap();
        let original = first.intern(b"original").unwrap();
        let closure = first.finish().unwrap();
        let before = std::fs::read(path(&root, &original.object).unwrap()).unwrap();
        let mut second =
            PoolBuilder::begin(&root, closure, budget(), format!("{:032x}", 2)).unwrap();
        assert_eq!(second.intern(b"original").unwrap(), original);
        let revised = second.intern(b"revised").unwrap();
        let closure = second.finish().unwrap();
        assert_eq!(closure.objects.len(), 2);
        assert_eq!(closure.objects[&revised.object].payloads.len(), 1);
        assert_eq!(
            std::fs::read(path(&root, &original.object).unwrap()).unwrap(),
            before
        );
        let reader = PoolReader::open(&root, closure.clone(), budget()).unwrap();
        assert_eq!(reader.read(&original).unwrap(), b"original");
        assert_eq!(reader.read(&revised).unwrap(), b"revised");
        drop(reader);
        let mut third =
            PoolBuilder::begin(&root, closure, budget(), format!("{:032x}", 3)).unwrap();
        third.intern(b"revised").unwrap();
        let closure = third.finish().unwrap();
        assert_eq!(closure.objects.len(), 1);
        assert!(!path(&root, &format!("{:032x}", 3)).unwrap().exists());
        let reader = PoolReader::open(&root, closure, budget()).unwrap();
        assert!(reader.read(&original).is_err());
        // Removing a reference is not permission to delete a pinned old object.
        assert!(path(&root, &original.object).unwrap().exists());
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn bounds_and_object_identity_are_checked_before_reuse() {
        let root = root();
        assert!(
            PoolBuilder::begin(&root, Closure::default(), budget(), "../elsewhere".into()).is_err()
        );
        let mut first =
            PoolBuilder::begin(&root, Closure::default(), budget(), format!("{:032x}", 1)).unwrap();
        let r = first.intern(b"original").unwrap();
        let closure = first.finish().unwrap();
        assert!(
            PoolReader::open(
                &root,
                closure.clone(),
                Budget {
                    bytes: 23,
                    ..budget()
                }
            )
            .is_err()
        );
        let mut second = PoolBuilder::begin(
            &root,
            closure,
            Budget {
                objects: 1,
                ..budget()
            },
            format!("{:032x}", 2),
        )
        .unwrap();
        assert_eq!(second.intern(b"original").unwrap(), r);
        assert!(second.intern(b"new").is_err());
        assert_eq!(second.finish().unwrap().objects.len(), 1);
        std::fs::remove_dir_all(root).unwrap();
    }
}
