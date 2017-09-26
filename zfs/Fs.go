package zfs

import (
	"fmt"
	"os/exec"
	"strings"
)

type Fs struct {
	zfs      *Zfs
	fullname string
	name     string
	children map[string]*Fs
	snaps    []string
}

func (f *Fs) MustGet(desc string) *Fs {
	subFs, err := f.Get(desc)
	if err != nil {
		panic(err)
	}

	return subFs
}

func (f *Fs) Get(desc string) (*Fs, error) {
	slash := strings.Index(desc, "/")
	if slash == -1 {
		return nil, fmt.Errorf("no slash found in '%s'", desc)
	}
	rootfsname := desc[0:slash]

	if f.name == rootfsname {
		return f.GetChild(desc[slash+1:])
	}
	return nil, fmt.Errorf("Name mismatch: %s != %s", f.name, rootfsname)
}

func (f *Fs) GetChild(desc string) (*Fs, error) {

	slash := strings.Index(desc, "/")
	fsname := desc
	if slash == -1 {
		if child, ok := f.children[fsname]; ok {
			return child, nil
		}
		return nil, fmt.Errorf("Unable to find %s in %s", fsname, f.fullname)
	}

	fsname = desc[0:slash]
	if child, ok := f.children[fsname]; ok {
		return child.GetChild(desc[slash+1:])
	}
	return nil, fmt.Errorf("Unable to find %s in %s", fsname, f.fullname)
}

func (f *Fs) String() string {
	s := f.name + "\n"

	for _, fs := range f.children {
		s = s + fs.doString(1)
	}

	return s
}

func (f *Fs) doString(level int) string {
	s := fmt.Sprintf("%s -> %s\n", strings.Repeat("  ", level), f.name)
	for _, snap := range f.snaps {
		s = s + fmt.Sprintf("%s @> %s\n", strings.Repeat("  ", level+2), snap)
	}

	for _, fs := range f.children {
		s = s + fs.doString(level+1)
	}

	return s
}

func (f *Fs) AddSnapshot(desc string) {
	comp := strings.Split(desc, "@")
	name := comp[0]
	snapname := comp[1]

	fs, e := f.Get(name)
	if e != nil {
		panic(e)
	} else {
		fs.snaps = append(fs.snaps, snapname)
	}
}

func (f *Fs) AddChild(desc string) {
	components := strings.Split(desc, "/")[1:]
	curf := f
	for i, v := range components {
		if val, ok := curf.children[v]; ok {
			curf = val
		} else {
			if len(components) != i+1 {
				panic("error: should be equal")
			}
			n := NewFs(f.zfs, desc)
			curf.children[v] = n
		}
	}
}

// Snapshots returns a list of all snapshots
func (f *Fs) Snapshots() []string {
	return f.snaps
}

func NewFs(z *Zfs, fullname string) *Fs {
	components := strings.Split(fullname, "/")
	name := components[len(components)-1]

	return &Fs{
		zfs:      z,
		fullname: fullname,
		name:     name,
		children: make(map[string]*Fs),
		snaps:    []string{},
	}
}

func (f *Fs) Recv(sendCommand *exec.Cmd) error {
	return f.zfs.Recv(f.fullname, sendCommand)
}
func (f *Fs) SendIncremental(prev, snap string) *exec.Cmd {
	return f.zfs.SendIncremental(f.fullname, prev, snap)
}
