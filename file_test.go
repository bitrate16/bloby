package bloby

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenClose(t *testing.T) {
	testDirName := "test-file-storage-TestOpenClose"

	t.Cleanup(func() {
		os.RemoveAll(testDirName)
	})

	storage, err := NewFileStorage(testDirName)
	assert.NoError(t, err)
	assert.NotNil(t, storage)

	err = storage.Open()
	assert.NoError(t, err)

	err = storage.Close()
	assert.NoError(t, err)

	assert.DirExists(t, testDirName)
}

func TestCreate(t *testing.T) {
	testDirName := "test-file-storage-TestCreate"

	t.Cleanup(func() {
		os.RemoveAll(testDirName)
	})

	storage, err := NewFileStorage(testDirName)
	assert.NoError(t, err)
	assert.NotNil(t, storage)

	err = storage.Open()
	assert.NoError(t, err)

	// Create full node
	node1, err := storage.Create(
		"AAAAAAAA",
		map[string]interface{}{
			"size":   "1K",
			"amount": 13,
			"tags": []string{
				"big",
				"tasty",
				"chesbargo",
			},
		},
	)
	assert.NoError(t, err)
	assert.NotNil(t, node1)
	assert.Equal(t, node1.GetName(), "AAAAAAAA")
	assert.NotNil(t, node1.GetMetadata())

	// Create node without metadata
	node2, err := storage.Create(
		"BBBBBBBB",
		nil,
	)
	assert.NoError(t, err)
	assert.NotNil(t, node2)
	assert.Equal(t, node2.GetName(), "BBBBBBBB")
	assert.Nil(t, node2.GetMetadata())

	// Check references not match
	assert.NotEqual(t, node1.GetReference(), node2.GetReference())

	// Try get by name
	node, err := storage.GetByName(node1.GetName())
	assert.NoError(t, err)
	assert.NotNil(t, node)
	assert.Equal(t, node.GetName(), "AAAAAAAA")
	assert.NotNil(t, node.GetMetadata())

	// Try get by reference
	node, err = storage.GetByReference(node2.GetReference())
	assert.NoError(t, err)
	assert.NotNil(t, node)
	assert.Equal(t, node.GetName(), "BBBBBBBB")
	assert.Nil(t, node.GetMetadata())

	// Try check existence by name
	exists, err := storage.ExistsByName("AAAAAAAA")
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = storage.ExistsByName("BBBBBBBB")
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = storage.ExistsByName("CCCCCCCC")
	assert.NoError(t, err)
	assert.False(t, exists)

	// Try check existence by reference
	exists, err = storage.ExistsByReference(node1.GetReference())
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = storage.ExistsByReference(node2.GetReference())
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = storage.ExistsByReference("NOEXISTENTREFERENCE")
	assert.NoError(t, err)
	assert.False(t, exists)

	err = storage.Close()
	assert.NoError(t, err)

	// Persistency check
	err = storage.Open()
	assert.NoError(t, err)

	exists, err = storage.ExistsByName("AAAAAAAA")
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = storage.ExistsByName("BBBBBBBB")
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = storage.ExistsByName("CCCCCCCC")
	assert.NoError(t, err)
	assert.False(t, exists)

	assert.DirExists(t, testDirName)
}
