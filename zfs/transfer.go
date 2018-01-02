package zfs

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	pb "gopkg.in/cheggaaa/pb.v1"
)

// Flags are options for a transfer process
type Flags struct {
	Recursive bool
	Force     bool
	Progress  bool
}

// Transfer is a set of arguments for transferring a single snapshot
type Transfer struct {
	Source           *Fs
	Destination      *Fs
	PreviousSnapshot string // can be empty
	CurrentSnapshot  string
	Flags            Flags
}

func (t *Transfer) recv() *exec.Cmd {
	// Build argument list
	args := []string{"recv"}
	if t.Flags.Force {
		// -F must be passed before the filesystem argument
		args = append(args, "-F")
	}
	args = append(args, t.Destination.fullname)

	return t.Destination.zfs.exec("/sbin/zfs", args...)
}

// send initializes the ZFS send command
func (t *Transfer) send() *exec.Cmd {
	return t.Source.zfs.Send(t.Source.fullname, t.PreviousSnapshot, t.CurrentSnapshot, false)
}

// sendSize retrieves the size of the snapshot diff
func (t *Transfer) sendSize() (int64, error) {
	cmd := t.Source.zfs.Send(t.Source.fullname, t.PreviousSnapshot, t.CurrentSnapshot, true)
	out, err := cmd.Output()

	if err != nil {
		return 0, err
	}

	buf := bytes.NewBuffer(out)
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

// Run performs sync
func (t *Transfer) Run() error {
	var err error
	var size int64

	if t.Flags.Progress {
		size, err = t.sendSize()
		if err != nil {
			return err
		}
	}

	recvCommand := t.recv()
	sendCommand := t.send()
	in, _ := recvCommand.StdinPipe()
	out, _ := sendCommand.StdoutPipe()

	log.Printf("Running %s | %s\n", strings.Join(sendCommand.Args, " "), strings.Join(recvCommand.Args, " "))

	mtx := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(3)

	// Start copy routine
	go func() {
		var e error
		if t.Flags.Progress {
			bar := pb.New64(size).SetUnits(pb.U_BYTES)
			bar.Start()
			_, e = io.Copy(in, bar.NewProxyReader(out))
			if e == nil {
				// Set to 100% percent
				bar.Set64(size)
			}
			bar.Finish()
		} else {
			_, e = io.Copy(in, out)
		}
		if e != nil {
			mtx.Lock()
			if err == nil {
				err = e
			}
			mtx.Unlock()
		}
		wg.Done()
	}()

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

// DoSync create missing file systems on the destination and transfers missing snapshots
func DoSync(from, to *Fs, flags Flags) error {
	log.Println("Synchronize", from.fullname, "to", to.fullname)

	// any snapshots to be transferred?
	if len(from.snaps) > 0 {
		transfer := Transfer{
			Source:      from,
			Destination: to,
			Flags:       flags,
		}

		var previous string
		var missing []string

		if len(to.snaps) == 0 {
			missing = from.snaps
		} else {
			common := lastCommonSnapshotIndex(from.snaps, to.snaps)
			if common == -1 {
				return fmt.Errorf("%s and %s don't have a common snapshot", from.fullname, to.fullname)
			}
			previous = from.snaps[common]
			missing = from.snaps[common+1:]
		}

		for _, current := range missing {
			transfer.PreviousSnapshot = previous
			transfer.CurrentSnapshot = current

			if err := transfer.Run(); err != nil {
				return err
			}
			previous = current
		}
	}

	// synchronize the children
	if flags.Recursive {
		for _, fromChild := range from.Children() {

			// ensure the filesystem exists
			toChild, err := to.CreateIfMissing(fromChild.name)
			if err != nil {
				return err
			}
			err = DoSync(fromChild, toChild, flags)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
