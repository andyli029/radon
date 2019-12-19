package output

import (
	"io"

	"volcano/execution"
)

type Output interface {
	WriteRecord(record *execution.Record) error
	io.Closer
}
