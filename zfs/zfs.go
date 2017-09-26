package zfs

import (
	"bufio"
	"fmt"
	"io"
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
	cmd := z.exec("/sbin/zfs", "list", "-t", "all", "-o", "name")
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
	scanner.Scan()

	l := scanner.Text()
	if l != "NAME" {
		return nil, fmt.Errorf("First line should be NAME: %s", l)
	}

	for scanner.Scan() {
		l := scanner.Text()
		isSnap := strings.Contains(l, "@")
		if f == nil {
			if isSnap {
				return nil, fmt.Errorf("First element should not be snapshot: %s", l)
			}

			f = NewFs(z, l)
		} else {
			if isSnap {
				f.AddSnapshot(l)
			} else {
				f.AddChild(l)
			}
		}

	}

	return f, nil
}

func (z *Zfs) Recv(fs string, sendCommand *exec.Cmd) error {
	cmd := z.exec("/sbin/zfs", "recv", fs)
	in, _ := cmd.StdinPipe()
	out, _ := sendCommand.StdoutPipe()

	fmt.Printf("Running %s | %s\n", strings.Join(sendCommand.Args, " "), strings.Join(cmd.Args, " "))

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

func (z *Zfs) SendIncremental(fs string, prev, snap string) *exec.Cmd {
	snapfqn := fmt.Sprintf("%s@%s", fs, snap)
	return z.exec("/sbin/zfs", "send", "-i", prev, snapfqn)
}

func DoSync(from, to *Fs) error {

	lastLocal := to.snaps[len(to.snaps)-1]

	remoteIndex := indexOf(from.snaps, lastLocal)

	missing := from.snaps[remoteIndex+1:]

	if len(missing) == 0 {
		fmt.Println("Nothing to do")
		return nil
	}

	fmt.Printf("last: %s, remoteIndex: %d, %s\n", lastLocal, remoteIndex, missing)

	prev := lastLocal

	for _, snap := range missing {
		err := to.Recv(from.SendIncremental(prev, snap))
		if err != nil {
			return err
		}
		prev = snap

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
