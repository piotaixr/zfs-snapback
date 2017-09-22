package zfs

import "os/exec"

type Exec func(string, ...string) *exec.Cmd

func LocalExec(name string, args ...string) *exec.Cmd {
	return exec.Command(name, args...)
}

func RemoteExec(dialstring string) Exec {
	return func(name string, args ...string) *exec.Cmd {
		return exec.Command("/usr/bin/ssh", append([]string{dialstring, name}, args...)...)
	}
}
