package bloby

import "io"

type Storage interface {
	GetByReference(reference string) (Node, error)
	GetByName(name string) (Node, error)
	Create(name string, metadata interface{}) (Node, error)
	Delete(reference string) error
	DeleteBy(namePrefix string, namePostfix string) error
	ExistsByName(name string) (bool, error)
	ExistsByReference(reference string) (bool, error)
	ListBy(namePrefix string, namePostfix string) ([]Node, error)
	ListReferences(namePrefix string, namePostfix string) ([]string, error)
	Open() error
	Close() error
}

type Node interface {
	GetReference() string
	GetName() string
	GetMetadata() interface{}
}

type TMutable interface {
	SetMetadata(metadata interface{}) error
	SetName(name string) error
}

type TPath interface {
	GetPath() string
}

type TInput interface {
	GetI() (io.Reader, error)
}

type IOutput interface {
	GetO() (io.Writer, error)
}
