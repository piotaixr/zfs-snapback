package main

import (
	"fmt"
	"os"
)

func main() {
	args := os.Args[1:]
	if len(args) < 4 {
		fmt.Printf("Expecting at 4 arguments (user, host, from, to), %d given \n", len(args))
		os.Exit(1)
	}
	fmt.Println("Listing local")
	lz := &zfs{}
	lf := lz.List()

	fmt.Println("Listing remote")
	rz := &remoteZfs{
		host: args[1],
		user: args[0],
	}
	rf := rz.List()

	from := args[2]
	to := args[3]

	DoSync(rf.MustGet(from), lf.MustGet(to))

}

func DoSync(from, to *Fs) {
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
