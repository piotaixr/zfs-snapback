package zfs

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

type Zfs interface {
	List() Fs
	Recv(fs string, sendCommand *exec.Cmd) error
	SendIncremental(fs, prev, snap string) *exec.Cmd
}

type zfs struct {
}

func New() Zfs {
	return &zfs{}
}

func (z *zfs) List() Fs {
	cmd := exec.Command("/sbin/zfs", "list", "-t", "all", "-o", "name")
	b, _ := cmd.Output()
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
func (z *zfs) Recv(fs string, sendCommand *exec.Cmd) error {
	cmd := exec.Command("/sbin/zfs", "recv", fs)
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
func (z *zfs) SendIncremental(fs string, prev, snap string) *exec.Cmd {
	panic("not implemented")
	return nil
}

type remoteZfs struct {
	host string
	user string
}

func NewRemote(host string, user string) Zfs {
	return &remoteZfs{
		host: host,
		user: user,
	}
}

func (z *remoteZfs) List() Fs {
	dialstring := fmt.Sprintf("%s@%s", z.user, z.host)
	cmd := exec.Command("/usr/bin/ssh", dialstring, "/sbin/zfs", "list", "-t", "all", "-o", "name")
	b, _ := cmd.Output()
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

func (z *remoteZfs) Recv(fs string, sendCommand *exec.Cmd) error {
	panic("not implemented")
	return errors.New("e")

}
func (z *remoteZfs) SendIncremental(fs string, prev, snap string) *exec.Cmd {
	dialstring := fmt.Sprintf("%s@%s", z.user, z.host)
	snapfqn := fmt.Sprintf("%s@%s", fs, snap)
	cmd := exec.Command("/usr/bin/ssh", dialstring, "-C", "/sbin/zfs", "send", "-i", prev, snapfqn)

	return cmd
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
