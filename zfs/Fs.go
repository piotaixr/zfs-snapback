package zfs

import (
	"fmt"
	"os/exec"
	"strings"
)

type Fs interface {
	GetChild(desc string) (Fs, error)
	MustGet(desc string) Fs
	Get(desc string) (Fs, error)
	String() string
	AddSnapshot(desc string)
	AddChild(desc string)
}

type fs struct {
	zfs      Zfs
	fullname string
	name     string
	children map[string]*fs
	snaps    []string
}

func (f *fs) MustGet(desc string) Fs {
	subFs, err := f.Get(desc)
	if err != nil {
		panic(err)
	}

	return subFs
}

func (f *fs) Get(desc string) (Fs, error) {
	return f.get(desc)
}
func (f *fs) get(desc string) (*fs, error) {
	slash := strings.Index(desc, "/")
	rootfsname := desc[0:slash]

	if f.name == rootfsname {
		return f.getChild(desc[slash+1:])
	}
	return nil, fmt.Errorf("Name mismatch: %s != %s", f.name, rootfsname)
}

func (f *fs) GetChild(desc string) (Fs, error) {
	return f.getChild(desc)
}

func (f *fs) getChild(desc string) (*fs, error) {
	// fmt.Printf("GetChild %s\n", desc)

	slash := strings.Index(desc, "/")
	fsname := desc
	if slash == -1 {
		if child, ok := f.children[fsname]; ok {
			return child, nil
		} else {
			return nil, fmt.Errorf("Unable to find %s in %s", fsname, f.fullname)
		}
	} else {
		fsname = desc[0:slash]
		if child, ok := f.children[fsname]; ok {
			return child.getChild(desc[slash+1:])
		} else {
			return nil, fmt.Errorf("Unable to find %s in %s", fsname, f.fullname)
		}
	}
}

func (f *fs) String() string {
	s := f.name + "\n"

	for _, fs := range f.children {
		s = s + fs.doString(1)
	}

	return s
}

func (f *fs) doString(level int) string {
	s := fmt.Sprintf("%s -> %s\n", strings.Repeat("  ", level), f.name)
	for _, snap := range f.snaps {
		s = s + fmt.Sprintf("%s @> %s\n", strings.Repeat("  ", level+2), snap)
	}

	for _, fs := range f.children {
		s = s + fs.doString(level+1)
	}

	return s
}

func (f *fs) AddSnapshot(desc string) {
	comp := strings.Split(desc, "@")
	name := comp[0]
	snapname := comp[1]

	fs, e := f.get(name)
	if e != nil {
		panic(e)
	} else {
		fs.snaps = append(fs.snaps, snapname)
	}
}

func (f *fs) AddChild(desc string) {
	components := strings.Split(desc, "/")[1:]
	curf := f
	for i, v := range components {
		// fmt.Println(v)
		if val, ok := curf.children[v]; ok {
			curf = val
		} else {
			if len(components) != i+1 {
				panic("error: should be equal")
			}
			n := newFs(f.zfs, desc)
			curf.children[v] = n
		}
	}
}

func NewFs(z Zfs, fullname string) Fs {
	return newFs(z, fullname)
}

func newFs(z Zfs, fullname string) *fs {
	components := strings.Split(fullname, "/")
	name := components[len(components)-1]

	return &fs{
		zfs:      z,
		fullname: fullname,
		name:     name,
		children: make(map[string]*fs),
		snaps:    []string{},
	}
}

func (f *fs) Recv(sendCommand *exec.Cmd) error {
	return f.zfs.Recv(f.fullname, sendCommand)
}
func (f *fs) SendIncremental(prev, snap string) *exec.Cmd {
	return f.zfs.SendIncremental(f.fullname, prev, snap)
}
