package zfs

import (
	"fmt"
	"os/exec"
	"sort"
	"strings"
)

// Fs represents a file system
type Fs struct {
	zfs      *Zfs
	fullname string
	name     string
	children map[string]*Fs
	snaps    []string
}

func newFs(z *Zfs, fullname string) *Fs {
	name := fullname

	if i := strings.LastIndexByte(name, '/'); i != -1 {
		name = name[i+1:]
	}

	return &Fs{
		zfs:      z,
		fullname: fullname,
		name:     name,
		children: make(map[string]*Fs),
		snaps:    []string{},
	}
}

// Children returns a sorted list of direct children
func (f *Fs) Children() (children []*Fs) {
	for _, child := range f.children {
		children = append(children, child)
	}

	sort.Slice(children, func(i, j int) bool {
		return children[i].name < children[j].name
	})

	return children
}

// GetChild searches the filesystem with the given path recursively and returns it
func (f *Fs) GetChild(fspath string) (*Fs, error) {
	var fsname string
	slash := strings.IndexByte(fspath, '/')
	if slash != -1 {
		fsname = fspath[0:slash]
	} else {
		fsname = fspath
	}

	child, _ := f.children[fsname]
	if child == nil {
		if f.fullname == "" {
			return nil, fmt.Errorf("Unable to find %s", fsname)
		}
		return nil, fmt.Errorf("Unable to find %s in %s", fsname, f.fullname)
	}

	if slash != -1 {
		// search recursively
		return child.GetChild(fspath[slash+1:])
	}

	return child, nil
}

// CreateIfMissing creates a filesystem if it does not exist yet
// The filesystem is returned if is exists or was created.
func (f *Fs) CreateIfMissing(name string) (*Fs, error) {
	if strings.ContainsRune(name, '/') {
		panic("slashes not allowed in names: " + name)
	}

	if fs, _ := f.GetChild(name); fs != nil {
		// exists already
		return fs, nil
	}

	fullpath := f.fullname + "/" + name
	if err := f.zfs.Create(fullpath); err != nil {
		return nil, err
	}

	fs := newFs(f.zfs, fullpath)
	f.children[name] = fs
	return fs, nil
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

func (f *Fs) addSnapshot(desc string) {
	comp := strings.Split(desc, "@")
	name := comp[0]
	snapname := comp[1]

	fs, e := f.GetChild(name)
	if e != nil {
		panic(e)
	} else {
		fs.snaps = append(fs.snaps, snapname)
	}
}

func (f *Fs) addChild(desc string) {
	components := strings.Split(desc, "/")
	if f.name != "" {
		components = components[1:]
	}

	curf := f
	for i, v := range components {
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

// Snapshots returns a list of all snapshots
func (f *Fs) Snapshots() []string {
	return f.snaps
}

// Recv performs the `zfs recv` command
func (f *Fs) Recv(sendCommand *exec.Cmd, force bool) error {
	return f.zfs.Recv(f.fullname, sendCommand, force)
}

// Send performs the `zfs send` command
func (f *Fs) Send(snap string) *exec.Cmd {
	return f.zfs.Send(f.fullname, snap)
}

// SendIncremental performs the `zfs send -i` command
func (f *Fs) SendIncremental(previous, current string) *exec.Cmd {
	return f.zfs.SendIncremental(f.fullname, previous, current)
}
