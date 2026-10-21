use std::{fs, io, path::Path};

/// Publish a prepared directory on the same filesystem. The destination must
/// not exist; callers must never replace an open/mapped generation in place.
pub(super) fn publish_directory(prepared: &Path, target: &Path) -> io::Result<()> {
    if prepared.parent() != target.parent() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "publication must remain in one directory",
        ));
    }
    #[cfg(unix)]
    {
        fs::File::open(prepared)?.sync_all()?;
        fs::rename(prepared, target)?;
        fs::File::open(
            target
                .parent()
                .ok_or_else(|| io::Error::other("missing parent"))?,
        )?
        .sync_all()
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
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "uncertified persistence platform",
        ))
    }
}
