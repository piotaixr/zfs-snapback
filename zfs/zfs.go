package zfs

import (
	"bufio"
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// Zfs is a wrapper for local or remote ZFS commands
type Zfs struct {
	exec Exec
}

func ParseLocation(flags Flags, location string) (z *Zfs, fspath string) {
	colon := strings.LastIndexByte(location, ':')

	if colon == -1 {
		z = &Zfs{
			exec: LocalExec,
		}
		fspath = location
	} else {
		z = &Zfs{
			exec: RemoteExecutor(flags, location[:colon]),
		}
		fspath = location[colon+1:]
	}

	return
}

func GetFilesystem(flags Flags, location string) (*Fs, error) {
	z, fspath := ParseLocation(flags, location)
	fs, err := z.List()
	if err != nil {
		return nil, err
	}

	return fs.GetChild(fspath)
}

// List returns all ZFS volumes and snapshots
func (z *Zfs) List() (*Fs, error) {
	cmd := z.exec("/sbin/zfs", "list", "-t", "all", "-Hr", "-o", "name")
	b, err := cmd.Output()
	if err != nil {
		if e := err.(*exec.ExitError); e != nil && len(e.Stderr) > 0 {
			// Add stderr to error message
			err = fmt.Errorf("%s: %s", err, strings.TrimSpace(string(e.Stderr)))
		}

		return nil, err
	}
	return z.parseList(b), nil
}

func (z *Zfs) parseList(b []byte) *Fs {
	root := newFs(z, "")
	scanner := bufio.NewScanner(bytes.NewReader(b))

	for scanner.Scan() {
		line := scanner.Text()
		if strings.ContainsRune(line, '@') {
			root.addSnapshot(line)
		} else {
			root.addChild(line)
		}
	}
	return root
}

// Create creates a new filesystem by its full path
func (z *Zfs) Create(fs string) error {
	_, err := z.exec("/sbin/zfs", "create", fs).Output()
	return err
}

// Send initializes a `zfs send` command
func (z *Zfs) Send(fs string, previous, current string, dry bool) *exec.Cmd {

	args := []string{"send"}

	if dry {
		args = append(args, "-nP")
	}

	if previous != "" {
		args = append(args, "-i", fmt.Sprintf("@%s", previous))
	}

	args = append(args, fmt.Sprintf("%s@%s", fs, current))

	return z.exec("/sbin/zfs", args...)
}

// Returns the index of the last common snapshot
func lastCommonSnapshotIndex(listA, listB []string) int {
	result := -1

	for i, name := range listA {
		if indexOf(listB, name) != -1 {
			result = i
		}
	}

	return result
}

func indexOf(list []string, needle string) int {
	for i, e := range list {
		if e == needle {
			return i
		}
	}

	return -1
}

func parseTransferSize(data []byte) (int64, error) {
	buf := bytes.NewBuffer(data)
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			return 0, errors.Wrap(err, "unable to extract snapshot size")
		}
		if strings.HasPrefix(line, "size\t") {
			i, err := strconv.ParseInt(line[5:len(line)-1], 10, 64)
			if err != nil {
				return 0, err
			}
			return i, nil
		}
	}
}
