// Copyright © 2017 NAME HERE <EMAIL ADDRESS>
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"

	"github.com/piotaixr/zfs-snapback/zfs"
	"github.com/spf13/cobra"
)

func pleaseSet(varname string) error {
	return fmt.Errorf("Please provide a value for the parameter %s", varname)
}

// syncCmd represents the sync command
var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: run,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if user == "" {
			return pleaseSet("user")
		}
		if host == "" {
			return pleaseSet("host")
		}
		if remoteFs == "" {
			return pleaseSet("remote")
		}
		if localFs == "" {
			return pleaseSet("local")
		}
		return nil
	},
}

var user string
var host string
var remoteFs string
var localFs string

func init() {
	syncCmd.PersistentFlags().StringVarP(&user, "user", "u", "", "Remote ssh user")
	syncCmd.PersistentFlags().StringVarP(&host, "host", "H", "", "Remote host")
	syncCmd.PersistentFlags().StringVarP(&remoteFs, "remote", "r", "", "Remote FS name")
	syncCmd.PersistentFlags().StringVarP(&localFs, "local", "l", "", "Local FS name")

	syncCmd.MarkPersistentFlagRequired("user")
	syncCmd.MarkPersistentFlagRequired("host")
	syncCmd.MarkPersistentFlagRequired("remote")
	syncCmd.MarkPersistentFlagRequired("local")

	RootCmd.AddCommand(syncCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// syncCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// syncCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func run(cmd *cobra.Command, args []string) {

	fmt.Println("Listing local")
	lz := zfs.NewLocal()
	lf, err := lz.List()
	checkError(err)

	fmt.Println("Listing remote")
	rz := zfs.NewRemote(host, user)
	rf, err := rz.List()
	checkError(err)

	from := remoteFs
	to := localFs

	checkError(zfs.DoSync(rf.MustGet(from), lf.MustGet(to)))
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
