# zfs-snapback
Small utility in Go to synchronize snapshots recursively a source to a destination.
Both source and destination can be a remote or local file system.

# Usage
Note that
- the given FS should already be present on the destination.
- You need SSH to be set up either via ssh agent or keyfile, user/password will not work
- Destination file systems will be reverted to the most recent snapshot before receiving the data (`zfs recv -F`).

Examples:

```
zfs-snapback root@source.tld:remote/zfs/fs/path local/fs/path
zfs-snapback one/local/fs another/local/fs
zfs-snapback local/fs/path root@your.tld:remote/zfs/fs/path
zfs-snapback root@source.tld:remote/zfs/fs/path root@destination.tld:zpool/backups/source.tld
```

# What is not done (and will maybe come in the future)

- Testing
- Complete error handling
- Use `zfs send -I` instead of multiple calls to `zfs send -i`
