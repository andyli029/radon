package execution

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/pkg/errors"

	"volcano/common"
)

type Field struct {
	Name common.VariableName
}

type metadata struct {
	undo      bool
	eventTime time.Time
}

type Record struct {
	metadata   metadata
	fieldNames []common.VariableName
	data       []common.Value
}

type RecordOption func(stream *Record)

func WithUndo() RecordOption {
	return func(r *Record) {
		r.metadata.undo = true
	}
}

func WithEventTime(eventTime time.Time) RecordOption {
	return func(r *Record) {
		r.metadata.eventTime = eventTime
	}
}

func WithMetadataFrom(base *Record) RecordOption {
	return func(r *Record) {
		r.metadata = base.metadata
	}
}

func NewRecord(fields []common.VariableName, data map[common.VariableName]common.Value, opts ...RecordOption) *Record {
	dataInner := make([]common.Value, len(fields))
	for i := range fields {
		dataInner[i] = data[fields[i]]
	}
	return NewRecordFromSlice(fields, dataInner, opts...)
}

func NewRecordFromSlice(fields []common.VariableName, data []common.Value, opts ...RecordOption) *Record {
	r := &Record{
		fieldNames: fields,
		data:       data,
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

func (r *Record) Data() []common.Value {
	return r.data
}

func (r *Record) Value(field common.VariableName) common.Value {
	if field.Source() == "sys" {
		switch field.Name() {
		case "undo":
			return common.MakeBool(r.IsUndo())
		case "event_time":
			return r.EventTime()
		default:
			return common.MakeNull()
		}
	}
	for i := range r.fieldNames {
		if r.fieldNames[i] == field {
			return r.data[i]
		}
	}
	return common.MakeNull()
}

func (r *Record) Fields() []Field {
	fields := make([]Field, 0)
	for _, fieldName := range r.fieldNames {
		fields = append(fields, Field{
			Name: fieldName,
		})
	}
	if !r.metadata.eventTime.IsZero() {
		fields = append(fields, Field{
			Name: common.NewVariableName("sys.event_time"),
		})
	}
	if r.IsUndo() {
		fields = append(fields, Field{
			Name: common.NewVariableName("sys.undo"),
		})
	}

	return fields
}

func (r *Record) AsVariables() common.Variables {
	out := make(common.Variables)
	for i := range r.fieldNames {
		out[r.fieldNames[i]] = r.data[i]
	}

	return out
}

func (r *Record) AsTuple() common.Tuple {
	return common.MakeTuple(r.data)
}

func (r *Record) Equal(other *Record) bool {
	myFields := r.Fields()
	otherFields := other.Fields()
	if len(myFields) != len(otherFields) {
		return false
	}

	for i := range myFields {
		if myFields[i] != otherFields[i] {
			return false
		}
		if !common.AreEqual(r.Value(myFields[i].Name), other.Value(myFields[i].Name)) {
			return false
		}
	}

	if !r.metadata.eventTime.Equal(other.metadata.eventTime) {
		return false
	}

	if r.metadata.undo != other.metadata.undo {
		return false
	}

	return true
}

func (r *Record) String() string {
	parts := make([]string, len(r.fieldNames))
	for i := range r.fieldNames {
		parts[i] = fmt.Sprintf("%s: %s", r.fieldNames[i].String(), r.data[i].String())
	}

	return fmt.Sprintf("{%s}", strings.Join(parts, ", "))
}

func (r *Record) IsUndo() bool {
	return r.metadata.undo
}

func (r *Record) EventTime() common.Value {
	return common.MakeTime(r.metadata.eventTime)
}

type RecordStream interface {
	Next() (*Record, error)
	io.Closer
}

var ErrEndOfStream = errors.New("end of stream")

var ErrNotFound = errors.New("not found")
