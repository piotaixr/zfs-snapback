package zfs

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strings"
)

type Zfs struct {
	exec Exec
}

// NewLocal creates a wrapper for local ZFS commands
func NewLocal() *Zfs {
	return &Zfs{
		exec: LocalExec,
	}
}

// NewRemote creates a wrapper for remote ZFS commands
func NewRemote(host string, user string) *Zfs {
	return &Zfs{
		exec: RemoteExecutor(fmt.Sprintf("%s@%s", user, host)),
	}
}

// List returns all ZFS volumes and snapshots
func (z *Zfs) List() (*Fs, error) {
	cmd := z.exec("/sbin/zfs", "list", "-t", "all", "-Hr", "-o", "name")
	b, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	return z.parseList(b)
}

func (z *Zfs) parseList(b []byte) (*Fs, error) {
	s := string(b)
	scanner := bufio.NewScanner(strings.NewReader(s))
	var f *Fs

	for scanner.Scan() {
		line := scanner.Text()
		if f == nil {
			f = NewFs(z, line)
		} else {
			if strings.ContainsRune(line, '@') {
				f.AddSnapshot(line)
			} else {
				f.AddChild(line)
			}
		}

	}

	return f, nil
}

// Create creates a new filesystem by its full path
func (z *Zfs) Create(fs string) error {
	_, err := z.exec("/sbin/zfs", "create", fs).Output()
	return err
}

func (z *Zfs) Recv(fs string, sendCommand *exec.Cmd) error {
	cmd := z.exec("/sbin/zfs", "recv", "-F", fs)
	in, _ := cmd.StdinPipe()
	out, _ := sendCommand.StdoutPipe()

	log.Printf("Running %s | %s\n", strings.Join(sendCommand.Args, " "), strings.Join(cmd.Args, " "))

	go io.Copy(in, out)

	err := sendCommand.Start()
	if err != nil {
		return err
	}

	err = cmd.Start()
	if err != nil {
		return err
	}

	err = sendCommand.Wait()
	if err != nil {
		return err
	}

	err = cmd.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (z *Zfs) Send(fs string, snap string) *exec.Cmd {
	return z.exec("/sbin/zfs", "send", fmt.Sprintf("%s@%s", fs, snap))
}

func (z *Zfs) SendIncremental(fs string, prev, current string) *exec.Cmd {
	return z.exec("/sbin/zfs", "send", "-i",
		fmt.Sprintf("@%s", prev),
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
func DoSync(from, to *Fs) error {
	log.Println("Synchronize", from.fullname, "to", to.fullname)

	// any snapshots to be transferred?
	if len(from.snaps) > 0 {
		if len(to.snaps) > 0 {
			common := lastCommonSnapshotIndex(from.snaps, to.snaps)

			// incremental transfer of missing snapshots
			current := from.snaps[common]
			missing := from.snaps[common+1:]

			for _, snap := range missing {
				err := to.Recv(from.SendIncremental(current, snap))
				if err != nil {
					return err
				}
				current = snap
			}
		} else {
			// transfer the first snapshot
			err := to.Recv(from.Send(from.snaps[0]))
			if err != nil {
				return err
			}
		}
	}

	// synchronize the children
	for _, fromChild := range from.children {

		// ensure the filesystem exists
		toChild, err := to.CreateIfMissing(fromChild.name)
		if err != nil {
			return err
		}
		err = DoSync(fromChild, toChild)
		if err != nil {
			return err
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
