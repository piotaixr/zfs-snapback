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
		exec: RemoteExecutor(fmt.Sprintf("%s@%s", host, user)),
	}
}

// List returns all ZFS volumes and snapshots
func (z *Zfs) List() Fs {
	cmd := z.exec("/sbin/zfs", "list", "-t", "all", "-o", "name")
	b, _ := cmd.Output()
	return z.parseList(b)
}

func (z *Zfs) parseList(b []byte) Fs {
	s := string(b)
	scanner := bufio.NewScanner(strings.NewReader(s))
	var f Fs
	scanner.Scan() // Discarding first row as it is the name of the column
	for scanner.Scan() {
		l := scanner.Text()
		isSnap := strings.Contains(l, "@")
		if f == nil {
			if isSnap {
				panic("First element should not be snapshot. Error.")
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

	return f
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

func DoSync(_from, _to Fs) {
	from, _ := _from.(*fs) // Ugly, to remove
	to, _ := _to.(*fs)     // Ugly, to remove

	lastLocal := to.snaps[len(to.snaps)-1]

	remoteIndex := indexOf(from.snaps, lastLocal)

	missing := from.snaps[remoteIndex+1:]

	if len(missing) == 0 {
		fmt.Println("Nothing to do")
		return
	}

	fmt.Printf("last: %s, remoteIndex: %d, %s\n", lastLocal, remoteIndex, missing)

	prev := lastLocal

	for _, snap := range missing {
		err := to.Recv(from.SendIncremental(prev, snap))
		if err != nil {
			panic(err)
		}
		prev = snap

	}
}

func indexOf(list []string, needle string) int {
	for i, e := range list {
		if e == needle {
			return i
		}
	}

	return -1
}
