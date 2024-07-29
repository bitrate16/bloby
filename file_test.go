package bloby

import (
	"io"
	"os"
	"strings"
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

	// Check interface implementation
	storageCast, ok := (interface{}(storage)).(Storage)
	assert.True(t, ok)
	assert.NotNil(t, storageCast)

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

	err = storage.Close()
	assert.NoError(t, err)

	assert.DirExists(t, testDirName)
}

// Check a fully contains b with repeats
func CheckFullContains(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	c := make([]bool, len(a), len(a))

	for ax := range len(a) {
		for bx := range len(a) {
			if !c[bx] && a[ax] == b[bx] {
				c[bx] = true
			}
		}
	}

	for index := range len(c) {
		if !c[index] {
			return false
		}
	}

	return true
}

func TestCheckFullContains(t *testing.T) {
	assert.True(
		t,
		CheckFullContains(
			[]string{},
			[]string{},
		),
	)

	assert.True(
		t,
		CheckFullContains(
			[]string{"a", "b", "c", "d", "e", "f", "g", "h"},
			[]string{"a", "b", "c", "d", "e", "f", "g", "h"},
		),
	)

	assert.True(
		t,
		CheckFullContains(
			[]string{"a", "b", "c", "d", "e", "f", "g", "h"},
			[]string{"e", "f", "g", "h", "a", "b", "c", "d"},
		),
	)

	assert.True(
		t,
		CheckFullContains(
			[]string{"e", "f", "g", "h", "a", "b", "c", "d"},
			[]string{"a", "b", "c", "d", "e", "f", "g", "h"},
		),
	)

	assert.True(
		t,
		CheckFullContains(
			[]string{"e", "f", "g", "a", "b", "c", "d", "h"},
			[]string{"a", "b", "c", "e", "f", "g", "h", "d"},
		),
	)

	assert.True(
		t,
		CheckFullContains(
			[]string{"e", "g", "h", "f", "a", "b", "c", "d"},
			[]string{"a", "c", "d", "b", "e", "f", "g", "h"},
		),
	)

	assert.False(
		t,
		CheckFullContains(
			[]string{"a"},
			[]string{"a", "b", "c", "d", "e", "f", "g", "h"},
		),
	)

	assert.False(
		t,
		CheckFullContains(
			[]string{},
			[]string{"a", "b", "c", "d", "e"},
		),
	)

	assert.False(
		t,
		CheckFullContains(
			[]string{"a", "b", "c", "d", "e", "f", "g", "h"},
			[]string{"a"},
		),
	)

	assert.False(
		t,
		CheckFullContains(
			[]string{"a", "b", "c", "d", "e"},
			[]string{},
		),
	)
}

func NodesToNames(nodes []Node) []string {
	var names []string
	for index := range len(nodes) {
		names = append(names, nodes[index].GetName())
	}

	return names
}

func NodesToReferences(nodes []Node) []string {
	var references []string
	for index := range len(nodes) {
		references = append(references, nodes[index].GetReference())
	}

	return references
}

func CheckQueriedNodes(t *testing.T, names []string, references []string, nodes []Node) {
	assert.NotNil(t, nodes)

	query_names := NodesToNames(nodes)
	query_references := NodesToReferences(nodes)

	// Check for match
	assert.True(t, CheckFullContains(names, query_names), "Queries node names must contain same names as test input")
	assert.True(t, CheckFullContains(references, query_references), "Queries references names must contain same references as test input")
}

func TestList(t *testing.T) {
	testDirName := "test-file-storage-TestList"

	t.Cleanup(func() {
		os.RemoveAll(testDirName)
	})

	storage, err := NewFileStorage(testDirName)
	assert.NoError(t, err)
	assert.NotNil(t, storage)

	err = storage.Open()
	assert.NoError(t, err)

	names := []string{
		"a",
		"da",
		"ac",
		"ab",
		"dab",
		"acb",
		"ab",
		"dab",
		"acb",
		"aa",
		"daa",
		"aca",
		"aab",
		"daab",
		"acab",
		"aba",
		"daba",
		"acba",
		"aaa",
		"daaa",
		"acaa",
		"aaab",
		"daaab",
		"acaab",
		"abaa",
		"dabaa",
		"acbaa",
		"aaaa",
		"daaaa",
		"acaaa",
		"aaaab",
		"daaaab",
		"acaaab",
		"abaaa",
		"dabaaa",
		"acbaaa",
		"aaaaa",
		"daaaaa",
		"acaaaa",
		"aaaaab",
		"daaaaab",
		"acaaaab",
		"abaaaa",
		"dabaaaa",
		"acbaaaa",
		"aaaaaa",
		"daaaaaa",
		"acaaaaa",
		"aaaaaab",
		"daaaaaab",
		"acaaaaab",
		"abaaaaa",
		"dabaaaaa",
		"acbaaaaa",
		"aaaaaaa",
		"daaaaaaa",
		"acaaaaaa",
		"aaaaaaab",
		"daaaaaaab",
		"acaaaaaab",
		"abaaaaaa",
		"dabaaaaaa",
		"acbaaaaaa",
		"aaaaaaaa",
		"daaaaaaaa",
		"acaaaaaaa",
		"aaaaaaaab",
		"daaaaaaaab",
		"acaaaaaaab",
		"abaaaaaaa",
		"dabaaaaaaa",
		"acbaaaaaaa",
		"aaaaaaaaa",
		"daaaaaaaaa",
		"acaaaaaaaa",
		"aaaaaaaaab",
		"daaaaaaaaab",
		"acaaaaaaaab",
		"abaaaaaaaa",
		"dabaaaaaaaa",
		"acbaaaaaaaa",
		"a          ",
		"           b",
		"ab          ",
		"a          b",
		"            ",
		"    a       ",
		"    ab      ",
		"    a b     ",
		"      b     ",
	}
	var references []string

	// a%
	var names_a []string
	var references_a []string
	// ab%
	var names_ab []string
	var references_ab []string
	// %b
	var names__b []string
	var references__b []string
	// a%b
	var names_a_b []string
	var references_a_b []string

	for index := range len(names) {
		name := names[index]

		node, err := storage.Create(
			name,
			map[string]interface{}{
				"name":  name,
				"index": index,
			},
		)

		assert.NoError(t, err)
		assert.NotNil(t, node)
		assert.Equal(t, name, node.GetName())
		assert.NotNil(t, node.GetMetadata())

		reference := node.GetReference()
		references = append(references, reference)
		// a%
		if strings.HasPrefix(name, "a") {
			names_a = append(names_a, name)
			references_a = append(references_a, reference)
		}
		// ab%
		if strings.HasPrefix(name, "ab") {
			names_ab = append(names_ab, name)
			references_ab = append(references_ab, reference)
		}
		// %b
		if strings.HasSuffix(name, "b") {
			names__b = append(names__b, name)
			references__b = append(references__b, reference)
		}
		// a%b
		if strings.HasPrefix(name, "a") && strings.HasSuffix(name[len("a"):], "b") {
			names_a_b = append(names_a_b, name)
			references_a_b = append(references_a_b, reference)
		}
	}

	// Check node list a%
	nodes, err := storage.ListBy("a", "")
	assert.NoError(t, err)
	CheckQueriedNodes(t, names_a, references_a, nodes)

	// Check node list ab%
	nodes, err = storage.ListBy("ab", "")
	assert.NoError(t, err)
	CheckQueriedNodes(t, names_ab, references_ab, nodes)

	// Check node list %b
	nodes, err = storage.ListBy("", "b")
	assert.NoError(t, err)
	CheckQueriedNodes(t, names__b, references__b, nodes)

	// Check node list a%b
	nodes, err = storage.ListBy("a", "b")
	assert.NoError(t, err)
	CheckQueriedNodes(t, names_a_b, references_a_b, nodes)

	// Check reference list a%
	references, err = storage.ListReferences("a", "")
	assert.NoError(t, err)
	assert.True(t, CheckFullContains(references, references_a))

	// Check reference list ab%
	references, err = storage.ListReferences("ab", "")
	assert.NoError(t, err)
	assert.True(t, CheckFullContains(references, references_ab))

	// Check reference list %b
	references, err = storage.ListReferences("", "b")
	assert.NoError(t, err)
	assert.True(t, CheckFullContains(references, references__b))

	// Check reference list a%b
	references, err = storage.ListReferences("a", "b")
	assert.NoError(t, err)
	assert.True(t, CheckFullContains(references, references_a_b))

	err = storage.Close()
	assert.NoError(t, err)

	assert.DirExists(t, testDirName)
}

func DropSingleElement(a []string, index int) []string {
	return append(a[:index], a[index+1:]...)
}

func DropElementsBy(a []string, prefix string, postfix string) []string {
	na := make([]string, 0)

	for _, value := range a {
		if !(strings.HasPrefix(value, prefix) && strings.HasSuffix(value[len(prefix):], postfix)) {
			na = append(na, value)
		}
	}

	return na
}

func TestDeleteUnordered(t *testing.T) {
	testDirName := "test-file-storage-TestDeleteUnordered"

	t.Cleanup(func() {
		os.RemoveAll(testDirName)
	})

	storage, err := NewFileStorage(testDirName)
	assert.NoError(t, err)
	assert.NotNil(t, storage)

	err = storage.Open()
	assert.NoError(t, err)

	names := []string{
		"aaaaaaaa",
		"aaaaaaaa",
		"aaaaaaaa",
		"bbbbbbbb",
		"bbbbbbbb",
		"cccccccc",
		"dddddddd",
		"dddddddd",
		"dddddddd",
		"eeeeeeee",
		"ffffffff",
		"gggggggg",
		"",
		"",
		"",
		" ",
	}
	var references []string

	for index := range len(names) {
		name := names[index]

		node, err := storage.Create(
			name,
			map[string]interface{}{
				"name":  name,
				"index": index,
			},
		)

		assert.NoError(t, err)
		assert.NotNil(t, node)
		assert.Equal(t, name, node.GetName())
		assert.NotNil(t, node.GetMetadata())

		references = append(references, node.GetReference())
	}

	deletions := []int{len(names) - 1, 0, 1, 5, 3, 2, 4, 1, 1, 1, 0, 0}

	for _, index := range deletions {

		// Drop from DB
		err = storage.Delete(references[index])
		assert.NoError(t, err)

		// Drop from locals
		names = DropSingleElement(names, index)
		references = DropSingleElement(references, index)

		// List
		nodes, err := storage.ListBy("", "")
		assert.NoError(t, err)

		// Validate
		query_names := NodesToNames(nodes)
		query_references := NodesToReferences(nodes)

		assert.True(t, CheckFullContains(names, query_names))
		assert.True(t, CheckFullContains(references, query_references))
	}

	err = storage.Close()
	assert.NoError(t, err)

	assert.DirExists(t, testDirName)
}

func TestDeleteLinear(t *testing.T) {
	testDirName := "test-file-storage-TestDeleteLinear"

	t.Cleanup(func() {
		os.RemoveAll(testDirName)
	})

	storage, err := NewFileStorage(testDirName)
	assert.NoError(t, err)
	assert.NotNil(t, storage)

	err = storage.Open()
	assert.NoError(t, err)

	names := []string{
		"aaaaaaaa",
		"aaaaaaaa",
		"aaaaaaaa",
		"bbbbbbbb",
		"bbbbbbbb",
		"cccccccc",
		"dddddddd",
		"dddddddd",
		"dddddddd",
		"eeeeeeee",
		"ffffffff",
		"gggggggg",
		"",
		"",
		"",
		" ",
	}
	var references []string

	for index := range len(names) {
		name := names[index]

		node, err := storage.Create(
			name,
			map[string]interface{}{
				"name":  name,
				"index": index,
			},
		)

		assert.NoError(t, err)
		assert.NotNil(t, node)
		assert.Equal(t, name, node.GetName())
		assert.NotNil(t, node.GetMetadata())

		references = append(references, node.GetReference())
	}

	for range len(names) {
		index := 0

		// Drop from DB
		err = storage.Delete(references[index])
		assert.NoError(t, err)

		// Drop from locals
		names = DropSingleElement(names, index)
		references = DropSingleElement(references, index)

		// List
		nodes, err := storage.ListBy("", "")
		assert.NoError(t, err)

		// Validate
		query_names := NodesToNames(nodes)
		query_references := NodesToReferences(nodes)

		assert.True(t, CheckFullContains(names, query_names))
		assert.True(t, CheckFullContains(references, query_references))
	}

	err = storage.Close()
	assert.NoError(t, err)

	assert.DirExists(t, testDirName)
}

func TestDeleteLinearLast(t *testing.T) {
	testDirName := "test-file-storage-TestDeleteLinearLast"

	t.Cleanup(func() {
		os.RemoveAll(testDirName)
	})

	storage, err := NewFileStorage(testDirName)
	assert.NoError(t, err)
	assert.NotNil(t, storage)

	err = storage.Open()
	assert.NoError(t, err)

	names := []string{
		"aaaaaaaa",
		"aaaaaaaa",
		"aaaaaaaa",
		"bbbbbbbb",
		"bbbbbbbb",
		"cccccccc",
		"dddddddd",
		"dddddddd",
		"dddddddd",
		"eeeeeeee",
		"ffffffff",
		"gggggggg",
		"",
		"",
		"",
		" ",
	}
	var references []string

	for index := range len(names) {
		name := names[index]

		node, err := storage.Create(
			name,
			map[string]interface{}{
				"name":  name,
				"index": index,
			},
		)

		assert.NoError(t, err)
		assert.NotNil(t, node)
		assert.Equal(t, name, node.GetName())
		assert.NotNil(t, node.GetMetadata())

		references = append(references, node.GetReference())
	}

	for range len(names) {
		index := len(names) - 1

		// Drop from DB
		err = storage.Delete(references[index])
		assert.NoError(t, err)

		// Drop from locals
		names = DropSingleElement(names, index)
		references = DropSingleElement(references, index)

		// List
		nodes, err := storage.ListBy("", "")
		assert.NoError(t, err)

		// Validate
		query_names := NodesToNames(nodes)
		query_references := NodesToReferences(nodes)

		assert.True(t, CheckFullContains(names, query_names))
		assert.True(t, CheckFullContains(references, query_references))
	}

	err = storage.Close()
	assert.NoError(t, err)

	assert.DirExists(t, testDirName)
}

func TestDeletePrefix(t *testing.T) {
	testDirName := "test-file-storage-TestDeletePrefix"

	t.Cleanup(func() {
		os.RemoveAll(testDirName)
	})

	storage, err := NewFileStorage(testDirName)
	assert.NoError(t, err)
	assert.NotNil(t, storage)

	err = storage.Open()
	assert.NoError(t, err)

	names := []string{
		"aaaaaaaa",
		"aaaaaaaa",
		"aaaaaaaa",
		"bbbbbbbb",
		"bbbbbbbb",
		"cccccccc",
		"dddddddd",
		"dddddddd",
		"dddddddd",
		"eeeeeeee",
		"ffffffff",
		"gggggggg",
		"aaaabbbb",
		"aaaabbbb",
		"aaaabbbb",
		"bbbbbbbb",
		"bbbbbbbb",
		"ccccbbbb",
		"ddddbbbb",
		"ddddbbbb",
		"ddddbbbb",
		"eeeebbbb",
		"ffffbbbb",
		"ggggbbbb",
		"",
		"",
		"",
		" ",
	}
	var references []string

	for index := range len(names) {
		name := names[index]

		node, err := storage.Create(
			name,
			map[string]interface{}{
				"name":  name,
				"index": index,
			},
		)

		assert.NoError(t, err)
		assert.NotNil(t, node)
		assert.Equal(t, name, node.GetName())
		assert.NotNil(t, node.GetMetadata())

		references = append(references, node.GetReference())
	}

	deletions := [][]string{
		{"aa", "aaaaaa"},
		{"ggggggg", "g"},
		{" ", " "},
		{"", "bb"},
		{"", ""},
		{"", "a"},
	}

	for _, pair := range deletions {
		// Drop from DB
		err = storage.DeleteBy(pair[0], pair[1])
		assert.NoError(t, err)

		// Drop from locals
		newNames := make([]string, 0)
		newReferences := make([]string, 0)

		for index, name := range names {
			if pair[0] == " " {
				return
			}
			if !(strings.HasPrefix(name, pair[0]) && strings.HasSuffix(name[len(pair[0]):], pair[1])) {
				newNames = append(newNames, names[index])
				newReferences = append(newReferences, references[index])
			}
		}

		names = newNames
		references = newReferences

		// List
		nodes, err := storage.ListBy("", "")
		assert.NoError(t, err)

		// Validate
		query_names := NodesToNames(nodes)
		query_references := NodesToReferences(nodes)

		assert.True(t, CheckFullContains(names, query_names))
		assert.True(t, CheckFullContains(references, query_references))
	}

	err = storage.Close()
	assert.NoError(t, err)

	assert.DirExists(t, testDirName)
}

func TestRename(t *testing.T) {
	testDirName := "test-file-storage-TestRename"

	t.Cleanup(func() {
		os.RemoveAll(testDirName)
	})

	storage, err := NewFileStorage(testDirName)
	assert.NoError(t, err)
	assert.NotNil(t, storage)

	err = storage.Open()
	assert.NoError(t, err)

	// Create full node
	node, err := storage.Create(
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
	assert.NotNil(t, node)
	assert.Equal(t, node.GetName(), "AAAAAAAA")
	assert.NotNil(t, node.GetMetadata())

	// Cast & rename
	mutableNode, ok := node.(Mutable)
	assert.True(t, ok)

	err = mutableNode.SetName("BBBBBBBB")
	assert.NoError(t, err)
	assert.Equal(t, "BBBBBBBB", node.GetName())

	// Now query again and check
	nodeRequeried, err := storage.GetByReference(node.GetReference())
	assert.NoError(t, err)
	assert.Equal(t, "BBBBBBBB", nodeRequeried.GetName())

	nodeRequeried, err = storage.GetByName("BBBBBBBB")
	assert.NoError(t, err)
	assert.Equal(t, node.GetReference(), nodeRequeried.GetReference())

	nodeRequeried, err = storage.GetByName("AAAAAAAA")
	assert.NoError(t, err)
	assert.Nil(t, nodeRequeried)

	exists, err := storage.ExistsByName("BBBBBBBB")
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = storage.ExistsByName("AAAAAAAA")
	assert.NoError(t, err)
	assert.False(t, exists)

	err = storage.Close()
	assert.NoError(t, err)

	assert.DirExists(t, testDirName)
}

func TestChangeMetadata(t *testing.T) {
	testDirName := "test-file-storage-TestChangeMetadata"

	t.Cleanup(func() {
		os.RemoveAll(testDirName)
	})

	storage, err := NewFileStorage(testDirName)
	assert.NoError(t, err)
	assert.NotNil(t, storage)

	err = storage.Open()
	assert.NoError(t, err)

	// Create full node
	node, err := storage.Create(
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
	assert.NotNil(t, node)
	assert.Equal(t, node.GetName(), "AAAAAAAA")
	assert.NotNil(t, node.GetMetadata())

	// Cast & change
	mutableNode, ok := node.(Mutable)
	assert.True(t, ok)

	// non-null -> non-null
	err = mutableNode.SetMetadata("new metadata set to string")
	assert.NoError(t, err)
	assert.Equal(t, "new metadata set to string", node.GetMetadata())

	// Now query again and check
	nodeRequeried, err := storage.GetByReference(node.GetReference())
	assert.NoError(t, err)
	assert.Equal(t, "new metadata set to string", nodeRequeried.GetMetadata())

	// non-null -> null
	err = mutableNode.SetMetadata(nil)
	assert.NoError(t, err)
	assert.Nil(t, node.GetMetadata())

	// Now query again and check
	nodeRequeried, err = storage.GetByReference(node.GetReference())
	assert.NoError(t, err)
	assert.Nil(t, nodeRequeried.GetMetadata())

	// null -> non-null
	err = mutableNode.SetMetadata(9.1)
	assert.NoError(t, err)
	assert.Equal(t, 9.1, node.GetMetadata())

	// Now query again and check
	nodeRequeried, err = storage.GetByReference(node.GetReference())
	assert.NoError(t, err)
	assert.Equal(t, 9.1, nodeRequeried.GetMetadata())

	err = storage.Close()
	assert.NoError(t, err)

	assert.DirExists(t, testDirName)
}

func TestFileIO(t *testing.T) {
	testDirName := "test-file-storage-TestFileIO"

	t.Cleanup(func() {
		os.RemoveAll(testDirName)
	})

	storage, err := NewFileStorage(testDirName)
	assert.NoError(t, err)
	assert.NotNil(t, storage)

	err = storage.Open()
	assert.NoError(t, err)

	// Create full node
	node, err := storage.Create(
		"cats",
		nil,
	)
	assert.NoError(t, err)
	assert.NotNil(t, node)

	MEOW := []byte("meow")

	// Writable
	writable, ok := node.(Writable)
	assert.True(t, ok)
	assert.NotNil(t, writable)

	writer, err := writable.GetWriter()
	assert.NoError(t, err)

	writer.Write(MEOW)
	if closeable, ok := writer.(io.Closer); ok {
		closeable.Close()
	}

	// Readable
	readable, ok := node.(Readable)
	assert.True(t, ok)
	assert.NotNil(t, readable)

	reader, err := readable.GetReader()
	assert.NoError(t, err)

	var buffer [1024]byte
	cnt, err := reader.Read(buffer[:])
	assert.NoError(t, err)

	assert.Equal(t, len(MEOW), cnt)
	assert.Equal(t, len(MEOW), len(buffer[:cnt]))
	assert.Equal(t, MEOW, buffer[:cnt])

	if closeable, ok := reader.(io.Closer); ok {
		closeable.Close()
	}

	err = storage.Close()
	assert.NoError(t, err)

	assert.DirExists(t, testDirName)
}
