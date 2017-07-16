# zfs-snapback
Small utility in Go to fetch back zfs snapshots from a remote server via SSH and zfs send/recv

# Usage
Note that
- the FS should already be present locally, the program only transfers missing snapshots from the last present locally to the last present remotely.
- You need SSH to be set up either via ssh agent or keyfile, user/password will not work
- If you modify the destination FS, the recv command will fail. Putting the local FS readonly can be a good way to ensure that the content is not modified.

Example:

```
zfs-snapback -u root -H your.tld -r remote/zfs/fs/path -l local/fs/path
```

# What is not done (and will maybe come in the future)

- Testing
- Complete error handling
- Use `zfs send -I` instead of multiple calls to `zfs send -i`
