package bloby

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var randomSource = rand.New(rand.NewSource(time.Now().UnixNano()))

func randomHexString(n int) string {
	b := make([]byte, (n + 1))

	if _, err := randomSource.Read(b); err != nil {
		panic(err)
	}

	return hex.EncodeToString(b)
}

func checkStorageIsNil(storage *FileStorage) {
	if storage == nil {
		panic("storage is nil")
	}
}

func checkNodeIsNil(node *FileNode) {
	if node == nil {
		panic("node is nil")
	}
}

type FileStorage struct {
	lock   sync.RWMutex
	isOpen bool
	path   string
	db     *sql.DB
}

func (s *FileStorage) initDB() {
	s.db.Exec("create table if not exists metadata (name text, reference text, metadata text)")
	s.db.Exec("create index if not exists idx_metadata_name on metadata(name)")
	s.db.Exec("create index if not exists idx_metadata_reference on metadata(reference)")
	s.db.Exec("create index if not exists idx_metadata_name_reference on metadata(name, reference)")
}

func (s *FileStorage) getPathByReference(reference string) string {
	return path.Join(s.path, reference[0:2], reference[2:4], reference[4:6], reference)
}

func (s *FileStorage) getDirByReference(reference string) string {
	return path.Join(s.path, reference[0:2], reference[2:4], reference[4:6])
}

func (s *FileStorage) deleteFileNode(reference string) {
	os.RemoveAll(s.getPathByReference(reference))
}

func NewFileStorage(path string) (*FileStorage, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	return &FileStorage{
		path:   absPath,
		isOpen: false,
	}, nil
}

func (storage *FileStorage) GetByReference(reference string) (Node, error) {
	checkStorageIsNil(storage)

	storage.lock.RLock()
	defer storage.lock.RUnlock()

	if !storage.isOpen {
		return nil, errors.New("storage is closed")
	}

	rows, err := storage.db.Query("select name, reference, metadata from metadata where reference = ?", reference)
	if err != nil {
		return nil, err
	}

	if !rows.Next() {
		return nil, nil
	}

	var resultName string
	var resultReference string
	var resultMetadataJson sql.NullString

	err = rows.Scan(&resultName, &resultReference, &resultMetadataJson)
	if err != nil {
		return nil, err
	}

	var node FileNode
	node.storage = storage
	node.name = resultName
	node.reference = resultReference

	if resultMetadataJson.Valid {
		err = json.Unmarshal([]byte(resultMetadataJson.String), &node.metadata)
		if err != nil {
			node.metadata = nil
		}
	}

	return &node, nil
}

func (storage *FileStorage) GetByName(name string) (Node, error) {
	checkStorageIsNil(storage)

	storage.lock.RLock()
	defer storage.lock.RUnlock()

	if !storage.isOpen {
		return nil, errors.New("storage is closed")
	}

	rows, err := storage.db.Query("select name, reference, metadata from metadata where name = ?", name)
	if err != nil {
		return nil, err
	}

	if !rows.Next() {
		return nil, nil
	}

	var resultName string
	var resultReference string
	var resultMetadataJson sql.NullString

	err = rows.Scan(&resultName, &resultReference, &resultMetadataJson)
	if err != nil {
		return nil, err
	}

	var node FileNode
	node.storage = storage
	node.name = resultName
	node.reference = resultReference

	if resultMetadataJson.Valid {
		err = json.Unmarshal([]byte(resultMetadataJson.String), &node.metadata)
		if err != nil {
			node.metadata = nil
		}
	}

	return &node, nil
}

func (storage *FileStorage) Create(name string, metadata interface{}) (Node, error) {
	checkStorageIsNil(storage)

	storage.lock.Lock()
	defer storage.lock.Unlock()

	if !storage.isOpen {
		return nil, errors.New("storage is closed")
	}

	node := FileNode{
		storage:   storage,
		reference: randomHexString(24),
		name:      name,
		metadata:  metadata,
	}

	metadataBytes, err := json.Marshal(node.metadata)
	if err != nil {
		_, err = storage.db.Exec("insert into metadata (name, reference, metadata) values (?, ?, null)", node.name, node.reference)
	} else {
		_, err = storage.db.Exec("insert into metadata (name, reference, metadata) values (?, ?, ?)", node.name, node.reference, string(metadataBytes))
	}

	if err != nil {
		return nil, err
	}

	return &node, nil
}

func (storage *FileStorage) Delete(reference string) error {
	checkStorageIsNil(storage)

	storage.lock.Lock()
	defer storage.lock.Unlock()

	if !storage.isOpen {
		return errors.New("storage is closed")
	}

	_, err := storage.db.Exec("delete from metadata where reference = ?", reference)

	if err != nil {
		return err
	}

	storage.deleteFileNode(reference)

	return nil
}

func (storage *FileStorage) DeleteBy(namePrefix string, namePostfix string) error {
	checkStorageIsNil(storage)

	storage.lock.Lock()
	defer storage.lock.Unlock()

	if !storage.isOpen {
		return errors.New("storage is closed")
	}

	rows, err := storage.db.Query("select reference from metadata where name like ?", namePrefix+"%"+namePostfix)
	if err != nil {
		return err
	}

	references := make([]string, 0)

	for rows.Next() {
		var resultReference string

		err = rows.Scan(&resultReference)
		if err != nil {
			return err
		}

		references = append(references, resultReference)
	}

	_, err = storage.db.Exec("delete from metadata where name like ?", namePrefix+"%"+namePostfix)

	if err != nil {
		return err
	}

	for _, reference := range references {
		storage.deleteFileNode(reference)
	}

	return nil
}

func (storage *FileStorage) ExistsByName(name string) (bool, error) {
	checkStorageIsNil(storage)

	storage.lock.RLock()
	defer storage.lock.RUnlock()

	if !storage.isOpen {
		return false, errors.New("storage is closed")
	}

	node, err := storage.GetByName(name)
	if err != nil {
		return false, err
	}

	return node != nil && node != (*FileNode)(nil), nil
}

func (storage *FileStorage) ExistsByReference(reference string) (bool, error) {
	checkStorageIsNil(storage)

	storage.lock.RLock()
	defer storage.lock.RUnlock()

	if !storage.isOpen {
		return false, errors.New("storage is closed")
	}

	node, err := storage.GetByReference(reference)
	if err != nil {
		return false, err
	}

	return node != nil && node != (*FileNode)(nil), nil
}

func (storage *FileStorage) ListBy(namePrefix string, namePostfix string) ([]Node, error) {
	checkStorageIsNil(storage)

	storage.lock.RLock()
	defer storage.lock.RUnlock()

	if !storage.isOpen {
		return nil, errors.New("storage is closed")
	}

	rows, err := storage.db.Query("select name, reference, metadata from metadata where name like ?", namePrefix+"%"+namePostfix)
	if err != nil {
		return nil, err
	}

	nodes := make([]Node, 0)

	for rows.Next() {
		var resultName string
		var resultReference string
		var resultMetadataJson sql.NullString

		err = rows.Scan(&resultName, &resultReference, &resultMetadataJson)
		if err != nil {
			return nil, err
		}

		var node FileNode
		node.storage = storage
		node.name = resultName
		node.reference = resultReference

		if resultMetadataJson.Valid {
			err = json.Unmarshal([]byte(resultMetadataJson.String), &node.metadata)
			if err != nil {
				node.metadata = nil
			}
		}

		nodes = append(nodes, &node)
	}

	return nodes, nil
}

func (storage *FileStorage) ListReferences(namePrefix string, namePostfix string) ([]string, error) {
	checkStorageIsNil(storage)

	storage.lock.RLock()
	defer storage.lock.RUnlock()

	if !storage.isOpen {
		return nil, errors.New("storage is closed")
	}

	rows, err := storage.db.Query("select reference from metadata where name like ?", namePrefix+"%"+namePostfix)
	if err != nil {
		return nil, err
	}

	references := make([]string, 0)

	for rows.Next() {
		var resultReference string

		err = rows.Scan(&resultReference)
		if err != nil {
			return nil, err
		}

		references = append(references, resultReference)
	}

	return references, nil
}

// Open FileStorage in goroutine-safe mode
//
// WARNING:
//
//	This implementation supports only multigoroutine access, but not the multiprocess access. opening database in multiprocess mode will cause database corruption.
func (storage *FileStorage) Open() error {
	checkStorageIsNil(storage)

	storage.lock.RLock()
	defer storage.lock.RUnlock()

	if storage.isOpen {
		return errors.New("storage is open")
	}

	os.Mkdir(storage.path, 0755)
	dbPath := filepath.Join(storage.path, "metadata.db")
	dbPath, err := filepath.Abs(dbPath)
	if err != nil {
		return err
	}

	db, err := sql.Open("sqlite3", "file:"+dbPath+"?mode=rwc&nolock=1")
	if err != nil {
		return err
	}
	storage.db = db
	storage.initDB()

	storage.isOpen = true

	return nil
}

func (storage *FileStorage) Close() error {
	checkStorageIsNil(storage)

	storage.lock.RLock()
	defer storage.lock.RUnlock()

	if !storage.isOpen {
		return errors.New("storage is closed")
	}

	err := storage.db.Close()

	storage.isOpen = false

	return err
}

type FileNode struct {
	storage   *FileStorage
	reference string
	name      string
	metadata  interface{}
}

func (node *FileNode) GetReference() string {
	checkNodeIsNil(node)

	return node.reference
}

func (node *FileNode) GetName() string {
	checkNodeIsNil(node)

	return node.name
}

func (node *FileNode) GetMetadata() interface{} {
	checkNodeIsNil(node)

	return node.metadata
}

func (node *FileNode) SetName(name string) error {
	checkNodeIsNil(node)

	node.storage.lock.Lock()
	defer node.storage.lock.Unlock()

	if !node.storage.isOpen {
		return errors.New("storage is closed")
	}

	_, err := node.storage.db.Exec("update or ignore metadata set name = ? where reference = ?", name, node.reference)
	if err != nil {
		return err
	}

	node.name = name

	return nil
}

func (node *FileNode) SetMetadata(metadata interface{}) error {
	checkNodeIsNil(node)

	node.storage.lock.Lock()
	defer node.storage.lock.Unlock()

	if !node.storage.isOpen {
		return errors.New("storage is closed")
	}

	if metadata == nil {
		_, err := node.storage.db.Exec("update or ignore metadata set metadata = null where reference = ?", node.reference)
		if err != nil {
			return err
		}
	} else {
		metadataBytes, err := json.Marshal(metadata)
		if err != nil {
			return err
		}

		_, err = node.storage.db.Exec("update or ignore metadata set metadata = ? where reference = ?", string(metadataBytes), node.reference)
		if err != nil {
			return err
		}
	}

	node.metadata = metadata

	return nil
}

func (node *FileNode) GetPath() string {
	checkNodeIsNil(node)

	return node.storage.getPathByReference(node.reference)
}

func (node *FileNode) GetReader() (io.Reader, error) {
	checkNodeIsNil(node)

	return os.Open(node.GetPath())
}

func (node *FileNode) GetWriter() (io.Writer, error) {
	checkNodeIsNil(node)

	os.MkdirAll(node.storage.getDirByReference(node.reference), 0755)
	return os.Create(node.GetPath())
}

func (node *FileNode) GetFlagWriter(flag int) (io.Writer, error) {
	checkNodeIsNil(node)

	os.MkdirAll(node.storage.getDirByReference(node.reference), 0755)
	return os.OpenFile(node.GetPath(), flag, 0755)
}
