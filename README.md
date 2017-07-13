# zfs-snapback
Small utility in Go to fetch back zfs snapshots from a remote server via SSH and zfs send/recv

# Usage
Note that the FS should already be present locally, the program only transfers missing snapshots from the last present locally to the last present remotely.

Example:

```
zfs-snapback root your.tld remote/zfs/fs/path local/fs/path
```

# What is not done (and will maybe come in the future)

- Testing
- Complete error handling
- Use `zfs send -I` instead of multiple calls to `zfs send -i`
