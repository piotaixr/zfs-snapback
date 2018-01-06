package zfs

import (
	"os/exec"
)

// Exec executes a command
type Exec func(string, ...string) *exec.Cmd

// LocalExec executes a command remotely via SSH
func LocalExec(name string, args ...string) *exec.Cmd {
	return exec.Command(name, args...)
}

// RemoteExecutor returns function that executes a command remotely via SSH
func RemoteExecutor(flags Flags, dialstring string) Exec {
	return func(name string, args ...string) *exec.Cmd {
		args = append([]string{dialstring, name}, args...)

		if flags.Compression != "" {
			args = append([]string{"-o", "Compression=" + flags.Compression}, args...)
		}

		return exec.Command("/usr/bin/ssh", args...)
	}
}
