package bloby

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	if storage == nil {
		return nil, errors.New("storage is nil")
	}

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
	if storage == nil {
		return nil, errors.New("storage is nil")
	}

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
	if storage == nil {
		return nil, errors.New("storage is nil")
	}

	storage.lock.Lock()
	defer storage.lock.Unlock()

	if !storage.isOpen {
		return nil, errors.New("storage is closed")
	}

	node := FileNode{
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
	if storage == nil {
		return errors.New("storage is nil")
	}

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
	if storage == nil {
		return errors.New("storage is nil")
	}

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
	if storage == nil {
		return false, errors.New("storage is nil")
	}

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
	if storage == nil {
		return false, errors.New("storage is nil")
	}

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
	if storage == nil {
		return nil, errors.New("storage is nil")
	}

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
	if storage == nil {
		return nil, errors.New("storage is nil")
	}

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

func (storage *FileStorage) Open() error {
	if storage == nil {
		return errors.New("storage is nil")
	}

	storage.lock.RLock()
	defer storage.lock.RUnlock()

	if storage.isOpen {
		return errors.New("storage is open")
	}

	os.Mkdir(storage.path, 0755)
	db, err := sql.Open("sqlite3", filepath.Join(storage.path, "metadata.db"))
	if err != nil {
		return err
	}
	storage.db = db
	storage.initDB()

	storage.isOpen = true

	return nil
}

func (storage *FileStorage) Close() error {
	if storage == nil {
		return errors.New("storage is nil")
	}

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
	reference string
	name      string
	metadata  interface{}
}

func (node *FileNode) GetReference() string {
	if node == nil {
		panic("node is nil")
	}

	return node.reference
}

func (node *FileNode) GetName() string {
	if node == nil {
		panic("node is nil")
	}

	return node.name
}

func (node *FileNode) GetMetadata() interface{} {
	if node == nil {
		panic("node is nil")
	}

	return node.metadata
}
