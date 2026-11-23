package flatjsonl

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bool64/progress"
	"github.com/klauspost/compress/zstd"
	gzip "github.com/klauspost/pgzip"
)

// NopFile indicates a no op file.
const NopFile = "<nop>"

// WriteReceiver can receive a row for processing.
type WriteReceiver interface {
	SetupKeys(keys []flKey) error
	ReceiveRow(seq int64, values []Value) error
	Close() error
}

// Writer dispatches rows to multiple receivers.
type Writer struct {
	receivers []WriteReceiver
	Progress  *progress.Progress

	// names maps a receiver to its destination label (file path/table).
	names map[WriteReceiver]string
	// types maps a receiver to its output kind (parquet, sqlite, ...).
	types map[WriteReceiver]string

	// setupErrs keeps SetupKeys failures per receiver.
	setupErrs map[WriteReceiver]error
	// rowErrs keeps the first ReceiveRow failure per receiver.
	rowErrs map[WriteReceiver]error
	// closeErrs keeps Close failures per receiver.
	closeErrs map[WriteReceiver]error
	// completedAt keeps finalization time per receiver.
	completedAt map[WriteReceiver]time.Time

	// runFailed marks that the batch itself did not complete, so finalized
	// targets remain independently receivable while unfinalized ones are not batched.
	runFailed bool
}

// Value encapsulates value of an allowed Type.
type Value struct {
	Dst       string
	Type      Type
	String    string
	Number    float64
	RawNumber string
	Bool      bool
}

// Format formats Value as string.
func (v Value) Format() string {
	switch v.Type { //nolint: exhaustive
	case TypeString:
		return v.String
	case TypeFloat:
		if len(v.RawNumber) > 0 {
			return v.RawNumber
		}

		return strconv.FormatFloat(v.Number, 'g', 5, 64)
	case TypeBool:
		return strconv.FormatBool(v.Bool)
	case TypeNull:
		return "NULL"
	case TypeAbsent:
		return "ABSENT"
	default:
		panic(fmt.Sprintf("BUG: don't know how to format: %+v", v))
	}
}

// SetupKeys configures writers, recording per-receiver setup failures without
// aborting the remaining receivers.
func (w *Writer) SetupKeys(keys []flKey) error {
	var errs []string

	for _, r := range w.receivers {
		if err := r.SetupKeys(keys); err != nil {
			w.setupErrs[r] = err
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ", "))
	}

	return nil
}

// ReceiveRow passes row to all receivers, recording per-receiver failures while
// still attempting the other receivers.
func (w *Writer) ReceiveRow(seq int64, values []Value) error {
	var errs []string

	for _, r := range w.receivers {
		if err := r.ReceiveRow(seq, values); err != nil {
			if _, ok := w.rowErrs[r]; !ok {
				w.rowErrs[r] = err
			}

			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ", "))
	}

	return nil
}

// Add adds another row receiver and registers its destination label and kind.
func (w *Writer) Add(r WriteReceiver) {
	w.AddTarget(r, "", "")
}

// AddTarget adds a row receiver with a destination label and output kind.
func (w *Writer) AddTarget(r WriteReceiver, target string, kind string) {
	if w.names == nil {
		w.names = map[WriteReceiver]string{}
		w.types = map[WriteReceiver]string{}
		w.setupErrs = map[WriteReceiver]error{}
		w.rowErrs = map[WriteReceiver]error{}
		w.closeErrs = map[WriteReceiver]error{}
		w.completedAt = map[WriteReceiver]time.Time{}
	}

	w.receivers = append(w.receivers, r)
	w.names[r] = target
	w.types[r] = kind

	if m, ok := r.(progress.MetricsExposer); ok {
		w.Progress.AddMetrics(m.Metrics()...)
	}
}

// MarkRunFailed flags that the batch processing itself failed. Finalized
// receivers stay independently receivable; unfinalized ones are not batched.
func (w *Writer) MarkRunFailed() {
	w.runFailed = true
}

// HasReceivers is true if there are receivers.
func (w *Writer) HasReceivers() bool {
	return len(w.receivers) > 0
}

// Close tries to close all receivers, recording per-receiver failures and
// completion times while still closing every receiver.
func (w *Writer) Close() error {
	var errs []string

	for _, r := range w.receivers {
		if err := r.Close(); err != nil {
			w.closeErrs[r] = err
			errs = append(errs, err.Error())

			continue
		}

		if _, failed := w.setupErrs[r]; !failed {
			if _, rowFailed := w.rowErrs[r]; !rowFailed {
				w.completedAt[r] = time.Now()
			}
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ", "))
	}

	return nil
}

// Report builds a manifest of what was actually written to every target.
func (w *Writer) Report() RunReport {
	rep := RunReport{
		Completed:   !w.runFailed && len(w.setupErrs) == 0 && len(w.rowErrs) == 0 && len(w.closeErrs) == 0,
		GeneratedAt: time.Now(),
		Targets:     make([]TargetReport, 0, len(w.receivers)),
	}

	for _, r := range w.receivers {
		var t TargetReport
		if rr, ok := r.(Reporter); ok {
			t = rr.Report()
		} else {
			t = TargetReport{Type: w.types[r], Target: w.names[r]}
		}

		if t.Type == "" {
			t.Type = w.types[r]
		}

		if t.Target == "" {
			t.Target = w.names[r]
		}

		switch {
		case w.setupErrs[r] != nil:
			t.Status = TargetStatusFailed
			t.Error = w.setupErrs[r].Error()
		case w.rowErrs[r] != nil:
			t.Status = TargetStatusFailed
			t.Error = w.rowErrs[r].Error()
		case w.closeErrs[r] != nil:
			t.Status = TargetStatusFailed
			t.Error = w.closeErrs[r].Error()
		case w.runFailed:
			if doneAt, ok := w.completedAt[r]; ok {
				// The artifact was finalized before/while the batch aborted,
				// it can be consumed on its own.
				t.Status = TargetStatusReceivable
				t.CompletedAt = &doneAt
			} else {
				// The target was never closed: its artifact is partial.
				t.Status = TargetStatusNotBatched
				t.Error = "batch failed before target was finalized"
			}
		default:
			doneAt := w.completedAt[r]
			t.Status = TargetStatusReceivable
			t.CompletedAt = &doneAt
		}

		rep.Targets = append(rep.Targets, t)
	}

	sort.Slice(rep.Targets, func(i, j int) bool {
		if rep.Targets[i].Target == rep.Targets[j].Target {
			return rep.Targets[i].Type < rep.Targets[j].Type
		}

		return rep.Targets[i].Target < rep.Targets[j].Target
	})

	return rep
}

type idxKey struct {
	idx int
	k   flKey
}

type baseWriter struct {
	p *Processor

	row          []string
	keyIndexes   []int   // Key indexes of this projection in incoming []Value.
	keys         []flKey // Full list of original keys.
	filteredKeys []flKey // Reduced list of keys for this projection.
	indexType    Type    // Type of transpose index (int for arrays, string for objects).

	isTransposed bool
	transposed   map[string]*baseWriter
	trimmedKeys  map[string]idxKey

	// transposedMapping maps original key index to reduced set of trimmed keys.
	transposedMapping map[int]int

	extName string
}

func (b *baseWriter) setupKeys(keys []flKey) {
	b.keys = keys

	for i, key := range keys {
		if key.transposeDst == "" {
			b.keyIndexes = append(b.keyIndexes, i)

			continue
		}

		tw := b.transposedWriter(key.transposeDst, keys)

		tw.keyIndexes = append(tw.keyIndexes, i)
		tw.indexType = key.transposeKey.t

		ik, ok := tw.trimmedKeys[key.transposeTrimmed]
		if !ok {
			ik.idx = len(tw.trimmedKeys)
			key.replaced = b.p.prepareKey(key.transposeTrimmed)
			ik.k = key
			tw.trimmedKeys[key.transposeTrimmed] = ik
		}

		tw.transposedMapping[i] = ik.idx
	}

	b.initFilteredKeys()

	for _, tw := range b.transposed {
		tw.initFilteredKeys()
	}
}

func (b *baseWriter) initFilteredKeys() {
	if !b.isTransposed {
		b.filteredKeys = make([]flKey, 0, len(b.keyIndexes))

		for _, i := range b.keyIndexes {
			b.filteredKeys = append(b.filteredKeys, b.keys[i])
		}

		return
	}

	b.filteredKeys = make([]flKey, len(b.trimmedKeys))
	for _, i := range b.trimmedKeys {
		b.filteredKeys[i.idx] = i.k
	}

	b.filteredKeys[0].t = TypeInt     // .sequence
	b.filteredKeys[1].t = b.indexType // .index

	for o, t := range b.transposedMapping {
		k := b.filteredKeys[t]
		k.UpdateType(b.keys[o].t)
		b.filteredKeys[t] = k
	}
}

func (b *baseWriter) transposedWriter(dst string, keys []flKey) *baseWriter {
	tw := b.transposed[dst]
	if tw != nil {
		return tw
	}

	if b.transposed == nil {
		b.transposed = map[string]*baseWriter{}
	}

	tw = &baseWriter{}

	tw.isTransposed = true
	tw.keys = keys
	tw.trimmedKeys = map[string]idxKey{
		"._sequence": {idx: 0, k: flKey{
			original: "._sequence",
			replaced: b.p.prepareKey("._sequence"),
		}},
		"._index": {idx: 1, k: flKey{
			original: "._index",
			replaced: b.p.prepareKey("._index"),
		}},
	}
	tw.transposedMapping = map[int]int{}

	b.transposed[dst] = tw

	return tw
}

func (b *baseWriter) transposedFileName(base string, dst string) string {
	dir, fn := path.Split(base)
	ext := path.Ext(fn)
	fn = fn[0 : len(fn)-len(ext)]
	fn = path.Join(dir, fn+"_"+dst+ext)

	return fn
}

func (b *baseWriter) receiveRow(seq int64, values []Value) (transposedRows [][]string) {
	if len(b.keys) != len(values) {
		panic(fmt.Sprintf("BUG: keys and values mismatch:\nKeys:\n%v\nValues:\n%v\n", b.keys, values))
	}

	b.row = b.row[:0]

	transposedRowsIdx := map[string][]string{}

	for _, i := range b.keyIndexes {
		v := values[i]

		var f string
		if v.Type != TypeNull && v.Type != TypeAbsent {
			f = v.Format()
		}

		if b.isTransposed {
			if v.Type == TypeAbsent {
				continue
			}

			k := b.keys[i]

			transposeKey := k.transposeKey.String()
			row := transposedRowsIdx[transposeKey]

			if row == nil {
				row = make([]string, len(b.trimmedKeys))
				row[0] = strconv.Itoa(int(seq)) // Add sequence.
				row[1] = transposeKey           // Add array idx/object property.
				transposedRowsIdx[transposeKey] = row
				transposedRows = append(transposedRows, row)
			}

			row[b.transposedMapping[i]] = f
		} else {
			b.row = append(b.row, f)
		}
	}

	return transposedRows
}

func (b *baseWriter) receiveTransposedRowValues(seq int64, values []Value) (transposedRows [][]Value) {
	if len(b.keys) != len(values) {
		panic(fmt.Sprintf("BUG: keys and values mismatch:\nKeys:\n%v\nValues:\n%v\n", b.keys, values))
	}

	if !b.isTransposed {
		return nil
	}

	transposedRowsIdx := map[string][]Value{}

	for _, i := range b.keyIndexes {
		v := values[i]

		if v.Type == TypeAbsent {
			continue
		}

		k := b.keys[i]

		transposeKey := k.transposeKey.String()
		row := transposedRowsIdx[transposeKey]

		if row == nil {
			row = make([]Value, len(b.trimmedKeys))
			row[0] = Value{
				Type:   TypeFloat,
				Number: float64(seq),
			} // Add sequence.
			row[1] = k.transposeKey.Value() // Add array idx/object property.
			transposedRowsIdx[transposeKey] = row
			transposedRows = append(transposedRows, row)
		}

		row[b.transposedMapping[i]] = v
	}

	return transposedRows
}

func (b *baseWriter) receiveRowValues(values []Value) (row []Value) {
	if len(b.keys) != len(values) {
		panic(fmt.Sprintf("BUG: keys and values mismatch:\nKeys:\n%v\nValues:\n%v\n", b.keys, values))
	}

	row = make([]Value, len(b.keyIndexes))

	for j, i := range b.keyIndexes {
		row[j] = values[i]
	}

	return row
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (nopWriter) Close() error {
	return nil
}

type fileWriter struct {
	f  io.WriteCloser
	fn string

	uncompressed *progress.CountingWriter
	compressed   *progress.CountingWriter
}

func newFileWriter(fn string) (*fileWriter, error) {
	var err error

	c := &fileWriter{}
	c.fn = fn

	if fn == NopFile {
		c.f = nopWriter{}
	} else {
		c.f, err = os.Create(fn)
		if err != nil {
			return nil, fmt.Errorf("failed to create file %s: %w", fn, err)
		}

		switch {
		case strings.HasSuffix(fn, ".gz"):
			c.compressed = progress.NewCountingWriter(c.f)
			c.f = gzip.NewWriter(c.compressed)
		case strings.HasSuffix(fn, ".zst"):
			c.compressed = progress.NewCountingWriter(c.f)

			c.f, err = zstd.NewWriter(c.compressed, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithLowerEncoderMem(true))
			if err != nil {
				return nil, err
			}
		}
	}

	c.uncompressed = progress.NewCountingWriter(c.f)

	return c, nil
}

// Metrics return available metrics.
func (c *fileWriter) Metrics() []progress.Metric {
	var res []progress.Metric

	if c.compressed != nil {
		res = append(res, progress.Metric{
			Name:  path.Base(c.fn) + " (comp)",
			Type:  progress.Bytes,
			Value: c.compressed.Bytes,
		})
	}

	if c.uncompressed != nil {
		res = append(res, progress.Metric{
			Name:  path.Base(c.fn),
			Type:  progress.Bytes,
			Value: c.uncompressed.Bytes,
		})
	}

	return res
}
