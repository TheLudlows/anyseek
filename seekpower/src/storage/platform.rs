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
            let mut value: Vec<u16> = path.as_os_str().encode_wide().collect();
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
