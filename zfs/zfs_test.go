package zfs

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseList(t *testing.T) {
	assert := assert.New(t)

	data, err := ioutil.ReadFile("testdata/zfs_list")
	assert.NoError(err)

	zfs := Zfs{}
	result := zfs.parseList(data)

	fs, err := result.GetChild("xx")
	assert.EqualError(err, "Unable to find xx")
	assert.Nil(fs)

	fs, err = result.GetChild("zroot/doesnotexist")
	assert.EqualError(err, "Unable to find doesnotexist in zroot")
	assert.Nil(fs)

	fs, err = result.GetChild("zroot")
	assert.NoError(err)
	assert.NotNil(fs)

	fs, err = fs.GetChild("ROOT")
	assert.NoError(err)
	assert.NotNil(fs)

	fs, err = fs.GetChild("default")
	assert.NoError(err)
	assert.NotNil(fs)
	assert.Equal([]string([]string{"2017-09-01T00:00", "2017-09-02T00:00"}), fs.Snapshots())

	fs, err = result.GetChild("zroot/var")
	assert.NoError(err)
	assert.NotNil(fs)
	assert.Len(fs.Snapshots(), 0)

	fs, err = fs.GetChild("cache")
	assert.NoError(err)
	assert.NotNil(fs)
	assert.Equal([]string([]string{"friday"}), fs.Snapshots())

	fs2, err := result.GetChild("zroot/var/cache")
	assert.NoError(err)
	assert.Equal(fs, fs2)
}

func TestParseLocation(t *testing.T) {
	assert := assert.New(t)
	var path string

	locations := []string{
		"foo/bar",
		"remote:foo/bar",
		"user@remote.host:foo/bar",
		"user@[2001::dead:beef]:foo/bar",
	}

	for _, loc := range locations {
		_, path = ParseLocation(Flags{}, loc)
		// it's not possible to compare function pointers :(
		assert.EqualValues("foo/bar", path, `for location "%s"`, loc)
	}
}

func TestLastCommonSnapshotIndex(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		listA    string
		listB    string
		expected int
	}{
		{"", "b c d", -1},
		{"a", "b c", -1},
		{"a", "a b", 0},
		{"a b", "a b", 1},
		{"a b c d", "b c d e f", 3},
	}

	for _, test := range tests {
		assert.Equal(test.expected, lastCommonSnapshotIndex(
			strings.Split(test.listA, " "),
			strings.Split(test.listB, " "),
		), `A="%s" B="%s"`, test.listA, test.listB)
	}

}
