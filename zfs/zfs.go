package zfs

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strings"
	"sync"
)

// Zfs is a wrapper for local or remote ZFS commands
type Zfs struct {
	exec Exec
}

func ParseLocation(location string) (z *Zfs, fspath string) {
	colon := strings.LastIndexByte(location, ':')

	if colon == -1 {
		z = &Zfs{
			exec: LocalExec,
		}
		fspath = location
	} else {
		z = &Zfs{
			exec: RemoteExecutor(location[:colon]),
		}
		fspath = location[colon+1:]
	}

	return
}

func GetFilesystem(location string) (*Fs, error) {
	z, fspath := ParseLocation(location)
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

// Recv performs the `zfs recv` command
func (z *Zfs) Recv(fs string, sendCommand *exec.Cmd, force bool) error {

	// Build argument list
	args := []string{"recv"}
	if force {
		// -F must be passed before the filesystem argument
		args = append(args, "-F")
	}
	args = append(args, fs)

	recvCommand := z.exec("/sbin/zfs", args...)
	in, _ := recvCommand.StdinPipe()
	out, _ := sendCommand.StdoutPipe()

	log.Printf("Running %s | %s\n", strings.Join(sendCommand.Args, " "), strings.Join(recvCommand.Args, " "))

	var err error
	mtx := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(2)

	// Start copy routine
	go io.Copy(in, out)

	// executes a command and closes the closer on failure
	run := func(cmd *exec.Cmd, closer io.Closer) {
		// capture stderr
		var stdErr bytes.Buffer
		cmd.Stderr = &stdErr

		// run the command
		if e := cmd.Run(); e != nil {
			mtx.Lock()
			defer mtx.Unlock()

			if err == nil {
				// It is the first failed process
				err = fmt.Errorf("%s failed with %v: %s", cmd.Args, e, stdErr.String())
			}

			// ensure the other process terminates
			closer.Close()
		}
		wg.Done()
	}

	// Start processes
	go run(recvCommand, out)
	go run(sendCommand, in)

	wg.Wait()
	return err
}

// Send performs the `zfs send` command
func (z *Zfs) Send(fs string, snap string) *exec.Cmd {
	return z.exec("/sbin/zfs", "send", fmt.Sprintf("%s@%s", fs, snap))
}

// SendIncremental performs the `zfs send -i` command
func (z *Zfs) SendIncremental(fs string, previous, current string) *exec.Cmd {
	return z.exec("/sbin/zfs", "send", "-i",
		fmt.Sprintf("@%s", previous),
		fmt.Sprintf("%s@%s", fs, current),
	)
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

// DoSync create missing file systems on the destination and transfers missing snapshots
func DoSync(from, to *Fs, recursive, force bool) error {
	log.Println("Synchronize", from.fullname, "to", to.fullname)

	// any snapshots to be transferred?
	if len(from.snaps) > 0 {
		if len(to.snaps) > 0 {
			common := lastCommonSnapshotIndex(from.snaps, to.snaps)

			if common == -1 {
				return fmt.Errorf("%s and %s don't have a common snapshot", from.fullname, to.fullname)
			}

			// incremental transfer of missing snapshots
			previous := from.snaps[common]
			missing := from.snaps[common+1:]

			for _, current := range missing {
				if err := to.Recv(from.SendIncremental(previous, current), force); err != nil {
					return err
				}
				previous = current
			}
		} else {
			// transfer the first snapshot
			if err := to.Recv(from.Send(from.snaps[0]), force); err != nil {
				return err
			}
		}
	}

	// synchronize the children
	if recursive {
		for _, fromChild := range from.Children() {

			// ensure the filesystem exists
			toChild, err := to.CreateIfMissing(fromChild.name)
			if err != nil {
				return err
			}
			err = DoSync(fromChild, toChild, recursive, force)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func indexOf(list []string, needle string) int {
	for i, e := range list {
		if e == needle {
			return i
		}
	}

	return -1
}
