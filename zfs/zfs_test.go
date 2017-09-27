package zfs

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseList(t *testing.T) {
	assert := assert.New(t)

	data, err := ioutil.ReadFile("testdata/zfs_list")
	assert.NoError(err)

	zfs := Zfs{}
	result, err := zfs.parseList(data)
	assert.NoError(err)
	assert.NotNil(result)

	fs, err := result.Get("xx")
	assert.EqualError(err, "Name mismatch: zroot != xx")
	assert.Nil(fs)

	fs, err = result.Get("zroot/doesnotexist")
	assert.EqualError(err, "Unable to find doesnotexist in zroot")
	assert.Nil(fs)

	fs, err = result.Get("zroot/ROOT")
	assert.NoError(err)
	assert.NotNil(fs)

	fs, err = result.GetChild("ROOT")
	assert.NoError(err)
	assert.NotNil(fs)

	fs, err = fs.GetChild("default")
	assert.NoError(err)
	assert.NotNil(fs)
	assert.Equal([]string([]string{"2017-09-01T00:00", "2017-09-02T00:00"}), fs.Snapshots())

	fs, err = result.GetChild("var")
	assert.NoError(err)
	assert.NotNil(fs)
	assert.Len(fs.Snapshots(), 0)

	fs, err = fs.GetChild("cache")
	assert.NoError(err)
	assert.NotNil(fs)
	assert.Equal([]string([]string{"friday"}), fs.Snapshots())

	fs2, err := result.Get("zroot/var/cache")
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
		_, path = ParseLocation(loc)
		// it's not possible to compare function pointers :(
		assert.EqualValues("foo/bar", path, `for location "%s"`, loc)
	}
}
