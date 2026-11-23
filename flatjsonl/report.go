package flatjsonl

import (
	"sort"
	"time"
)

// Target statuses recorded in the run report.
const (
	// TargetStatusReceivable means the target was finalized successfully and can
	// be consumed independently, even when other targets of the same run failed.
	TargetStatusReceivable = "independently_receivable"

	// TargetStatusNotBatched means the run did not complete as a batch and this
	// target was not finalized (closed), so its artifact is partial/incomplete.
	TargetStatusNotBatched = "not_batched"

	// TargetStatusFailed means the target itself failed (setup, row writing or close).
	TargetStatusFailed = "failed"
)

// FieldInfo describes a single field (column) written by a target.
type FieldInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// PartitionReport describes a physical or logical partition of an output:
// a transposed table/file, a wide-table split (_partN) or a row group.
type PartitionReport struct {
	// Name is the table name, file path or row group label.
	Name string `json:"name"`

	// Kind distinguishes partition shapes: "transpose", "column_split",
	// "row_group" or "primary".
	Kind string `json:"kind"`

	// RowsWritten is the number of rows actually written into this partition.
	RowsWritten int64 `json:"rows_written"`

	// RowGroups is the number of physical row groups (Parquet), 0 when not applicable.
	RowGroups int `json:"row_groups,omitempty"`

	// Fields are the columns of this partition.
	Fields []FieldInfo `json:"fields,omitempty"`
}

// TargetReport captures what was actually written to one output target.
type TargetReport struct {
	// Type is the receiver kind, e.g. "parquet", "sqlite", "csv".
	Type string `json:"type"`

	// Target is the file path, database file or other destination requested.
	Target string `json:"target"`

	// Status is one of TargetStatus* constants.
	Status string `json:"status"`

	// RowsWritten is the total number of rows actually written (including partitions).
	RowsWritten int64 `json:"rows_written"`

	// Fields are the columns of the primary (non-transposed) output.
	Fields []FieldInfo `json:"fields,omitempty"`

	// Partitions lists transposed outputs, wide-table splits and other partitions.
	Partitions []PartitionReport `json:"partitions,omitempty"`

	// CompletedAt is the time when the target was successfully closed/finalized.
	// It is nil for targets that did not reach completion.
	CompletedAt *time.Time `json:"completed_at,omitempty"`

	// Error holds the failure message when Status is failed or not_batched.
	Error string `json:"error,omitempty"`
}

// RunReport is a manifest of all output targets of a single run.
type RunReport struct {
	// Completed indicates whether the whole batch finished without errors.
	Completed bool `json:"completed"`

	// GeneratedAt is the time the report was produced.
	GeneratedAt time.Time `json:"generated_at"`

	// Targets lists reports keyed by destination, ordered by target name.
	Targets []TargetReport `json:"targets"`
}

// Reporter is an optional capability of a WriteReceiver to expose what was
// actually written. Receivers without it get a best-effort generic report.
type Reporter interface {
	Report() TargetReport
}

// fieldInfos converts a list of keys into field descriptors.
func fieldInfos(keys []flKey) []FieldInfo {
	res := make([]FieldInfo, 0, len(keys))

	for _, k := range keys {
		t := k.t
		if t == TypeAbsent {
			t = TypeString
		}

		res = append(res, FieldInfo{
			Name: k.replaced,
			Type: string(t),
		})
	}

	return res
}

// baseFields safely returns the filtered keys of a baseWriter, tolerating a
// receiver that failed before SetupKeys initialized it.
func baseFields(b *baseWriter) []FieldInfo {
	if b == nil {
		return nil
	}

	return fieldInfos(b.filteredKeys)
}

// sortedReportKeys returns map keys in deterministic order.
func sortedReportKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}
