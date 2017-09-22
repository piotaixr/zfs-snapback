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

	fs, err := result.Get("doesnot/exist")
	assert.EqualError(err, "Name mismatch: zroot != doesnot")
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
}

func TestParseListInvalid(t *testing.T) {
	assert := assert.New(t)
	zfs := Zfs{}
	var err error

	_, err = zfs.parseList([]byte("foo"))
	assert.EqualError(err, "First line should be NAME: foo")

	_, err = zfs.parseList([]byte("NAME\nsnap@shot"))
	assert.EqualError(err, "First element should not be snapshot: snap@shot")
}
