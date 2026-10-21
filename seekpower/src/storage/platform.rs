use std::{fs, io, path::Path};

#[derive(Debug)]
pub(super) enum PublishError {
    BeforeRename(io::Error),
    RenameAttempted(io::Error),
}
impl From<io::Error> for PublishError {
    fn from(error: io::Error) -> Self {
        Self::BeforeRename(error)
    }
}

/// Publish a prepared directory on the same filesystem. The destination must
/// not exist; callers must never replace an open/mapped generation in place.
pub(super) fn publish_directory(prepared: &Path, target: &Path) -> Result<(), PublishError> {
    if prepared.parent() != target.parent() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "publication must remain in one directory",
        )
        .into());
    }
    #[cfg(unix)]
    {
        fs::File::open(prepared)?.sync_all()?;
        fs::rename(prepared, target).map_err(PublishError::RenameAttempted)?;
        #[cfg(test)]
        fail_after_rename()?;
        fs::File::open(
            target
                .parent()
                .ok_or_else(|| PublishError::RenameAttempted(io::Error::other("missing parent")))?,
        )
        .map_err(PublishError::RenameAttempted)?
        .sync_all()
        .map_err(PublishError::RenameAttempted)
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use winapi::um::winbase::{MOVEFILE_WRITE_THROUGH, MoveFileExW};
        let wide = |path: &Path| -> io::Result<Vec<u16>> {
            // std::fs accepts long Windows paths, but the raw Win32 call below
            // needs an extended-length absolute path. Canonicalize the existing
            // parent (the destination itself does not yet exist), preserving
            // same-volume rename and WRITE_THROUGH rather than falling back to
            // copying. This also handles relative roots and UNC parents.
            let parent = path
                .parent()
                .ok_or_else(|| io::Error::other("missing publication parent"))?;
            let parent = if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            };
            let parent = parent.canonicalize()?;
            let absolute = parent.join(
                path.file_name()
                    .ok_or_else(|| io::Error::other("missing publication name"))?,
            );
            let mut value: Vec<u16> = absolute.as_os_str().encode_wide().collect();
            if value.contains(&0) {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "NUL in path"));
            }
            value.push(0);
            Ok(value)
        };
        let source = wide(prepared)?;
        let destination = wide(target)?;
        // No REPLACE_EXISTING or COPY_ALLOWED: fail on existing destinations
        // and cross-volume moves, rather than silently changing the protocol.
        if unsafe {
            MoveFileExW(
                source.as_ptr(),
                destination.as_ptr(),
                MOVEFILE_WRITE_THROUGH,
            )
        } == 0
        {
            return Err(PublishError::RenameAttempted(io::Error::last_os_error()));
        }
        #[cfg(test)]
        fail_after_rename()?;
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "uncertified persistence platform",
        )
        .into())
    }
}

#[cfg(test)]
fn fail_after_rename() -> Result<(), PublishError> {
    if std::env::var("SEEKPOWER_PUBLISH_FAIL").as_deref() == Ok("after_rename") {
        return Err(PublishError::RenameAttempted(io::Error::other(
            "injected post-rename synchronization failure",
        )));
    }
    Ok(())
}
