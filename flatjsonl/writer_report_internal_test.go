package flatjsonl

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubReceiver is a controllable WriteReceiver for report classification tests.
type stubReceiver struct {
	name     string
	rows     int64
	setupErr error
	rowErr   error
	closeErr error
	closed   bool
}

func (s *stubReceiver) SetupKeys([]flKey) error { return s.setupErr }

func (s *stubReceiver) ReceiveRow(seq int64, _ []Value) error {
	if s.rowErr != nil {
		return s.rowErr
	}

	s.rows++

	return nil
}

func (s *stubReceiver) Close() error {
	s.closed = true

	return s.closeErr
}

func (s *stubReceiver) Report() TargetReport {
	return TargetReport{
		Type:        "stub",
		Target:      s.name,
		RowsWritten: s.rows,
		Fields:      []FieldInfo{{Name: "a", Type: "string"}},
		Partitions: []PartitionReport{{
			Name:        s.name,
			Kind:        "primary",
			RowsWritten: s.rows,
		}},
	}
}

var errStub = errors.New("stub failure")

func TestWriter_Report_batchFailureClassifiesTargets(t *testing.T) {
	good := &stubReceiver{name: "good"}
	bad := &stubReceiver{name: "bad", rowErr: errStub}
	unclosed := &stubReceiver{name: "unclosed"}

	w := &Writer{}
	w.AddTarget(good, "good", "stub")
	w.AddTarget(bad, "bad", "stub")
	w.AddTarget(unclosed, "unclosed", "stub")

	require.NoError(t, w.SetupKeys(nil))

	// First row: bad fails, others receive; run aborts afterward.
	err := w.ReceiveRow(1, nil)
	require.Error(t, err)

	// Simulate the processor closing everything after the batch aborted.
	w.MarkRunFailed()
	require.NoError(t, w.Close())

	rep := w.Report()
	assert.False(t, rep.Completed)
	require.Len(t, rep.Targets, 3)

	byName := map[string]TargetReport{}
	for _, tg := range rep.Targets {
		byName[tg.Target] = tg
	}

	// Good target closed cleanly despite the failed batch: independently receivable.
	g := byName["good"]
	assert.Equal(t, TargetStatusReceivable, g.Status)
	assert.Equal(t, int64(1), g.RowsWritten)
	assert.NotNil(t, g.CompletedAt)

	// Bad target failed on its own.
	b := byName["bad"]
	assert.Equal(t, TargetStatusFailed, b.Status)
	assert.Contains(t, b.Error, "stub failure")
	assert.Nil(t, b.CompletedAt)

	// Unclosed target never received the row and was closed only during teardown;
	// because the batch failed and it had no row error but is finalized, it is
	// still independently receivable. A genuinely not-batched target is modeled
	// in the next test by skipping Close.
	u := byName["unclosed"]
	assert.Equal(t, TargetStatusReceivable, u.Status)
	assert.True(t, unclosed.closed)
}

func TestWriter_Report_notBatchedWhenNeverFinalized(t *testing.T) {
	good := &stubReceiver{name: "good"}
	pending := &stubReceiver{name: "pending"}

	w := &Writer{}
	w.AddTarget(good, "good", "stub")
	w.AddTarget(pending, "pending", "stub")

	require.NoError(t, w.SetupKeys(nil))
	require.NoError(t, w.ReceiveRow(1, nil))

	// Batch aborts; Close is invoked only for the good receiver (e.g. it owns a
	// resource that gets flushed), while pending is abandoned without Close.
	w.MarkRunFailed()
	require.NoError(t, good.Close())
	w.completedAt[good] = time.Now()

	rep := w.Report()
	assert.False(t, rep.Completed)

	byName := map[string]TargetReport{}
	for _, tg := range rep.Targets {
		byName[tg.Target] = tg
	}

	assert.Equal(t, TargetStatusReceivable, byName["good"].Status)

	p := byName["pending"]
	assert.Equal(t, TargetStatusNotBatched, p.Status)
	assert.NotEmpty(t, p.Error)
	assert.Nil(t, p.CompletedAt)
}

func TestWriter_Report_setupAndCloseFailure(t *testing.T) {
	setupBad := &stubReceiver{name: "setup", setupErr: errStub}
	closeBad := &stubReceiver{name: "closebad", closeErr: errStub}
	ok := &stubReceiver{name: "ok"}

	w := &Writer{}
	w.AddTarget(setupBad, "setup", "stub")
	w.AddTarget(closeBad, "closebad", "stub")
	w.AddTarget(ok, "ok", "stub")

	// Setup errors are isolated per receiver and do not stop the others.
	setupErr := w.SetupKeys(nil)
	require.Error(t, setupErr)

	// Rows still go to the receivers that set up successfully.
	require.NoError(t, w.ReceiveRow(1, nil))

	closeErr := w.Close()
	require.Error(t, closeErr)

	rep := w.Report()
	assert.False(t, rep.Completed)

	byName := map[string]TargetReport{}
	for _, tg := range rep.Targets {
		byName[tg.Target] = tg
	}

	assert.Equal(t, TargetStatusFailed, byName["setup"].Status)
	assert.Equal(t, TargetStatusFailed, byName["closebad"].Status)
	assert.Equal(t, TargetStatusReceivable, byName["ok"].Status)
	assert.Equal(t, int64(1), byName["ok"].RowsWritten)
}

// A real receiver that never ran SetupKeys must still produce a Report without
// panicking, because its base state was never initialized.
func TestWriter_Report_failedReceiverDoesNotPanic(t *testing.T) {
	pw, err := NewParquetWriter(NopFile, "snappy", &Processor{})
	require.NoError(t, err)
	require.NotPanics(t, func() {
		rep := pw.Report()
		assert.Equal(t, "parquet", rep.Type)
		assert.Equal(t, int64(0), rep.RowsWritten)
	})

	cw, err := NewCSVWriter(NopFile, "")
	require.NoError(t, err)
	require.NotPanics(t, func() { _ = cw.Report() })

	rw, err := NewRawWriter(NopFile, ",")
	require.NoError(t, err)
	require.NotPanics(t, func() { _ = rw.Report() })
}
