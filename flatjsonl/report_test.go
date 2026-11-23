package flatjsonl_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vearutop/flatjsonl/flatjsonl"
)

func loadReportConfig(t *testing.T) flatjsonl.Config {
	t.Helper()

	b, err := os.ReadFile("testdata/config.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config
	require.NoError(t, json.Unmarshal(b, &cfg))

	return cfg
}

func reportFlags(out string) flatjsonl.Flags {
	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "testdata/test.log"
	f.MatchLinePrefix = `([\w\d-]+) [\w\d]+ ([\d/]+\s[\d:\.]+)`
	f.MaxLines = 3
	f.ReplaceKeys = true
	f.SkipZeroCols = true
	f.Concurrency = 1
	f.Report = out

	return f
}

func readRunReport(t *testing.T, fn string) flatjsonl.RunReport {
	t.Helper()

	b, err := os.ReadFile(fn)
	require.NoError(t, err)

	var rep flatjsonl.RunReport
	require.NoError(t, json.Unmarshal(b, &rep))

	return rep
}

func findTarget(t *testing.T, rep flatjsonl.RunReport, target string) flatjsonl.TargetReport {
	t.Helper()

	for _, tg := range rep.Targets {
		if tg.Target == target {
			return tg
		}
	}

	t.Fatalf("target %q not found in report: %+v", target, rep.Targets)

	return flatjsonl.TargetReport{}
}

func TestRunReport_parquetAndSQLite(t *testing.T) {
	reportFn := "testdata/run-report.json"
	parquetFn := "testdata/report.parquet"
	sqliteFn := "testdata/report.sqlite"

	require.NoError(t, os.RemoveAll(reportFn))
	require.NoError(t, os.RemoveAll(parquetFn))
	require.NoError(t, os.RemoveAll(sqliteFn))
	t.Cleanup(func() {
		_ = os.Remove(reportFn)
		_ = os.Remove(parquetFn)
		_ = os.Remove(sqliteFn)
	})

	f := reportFlags(reportFn)
	f.Parquet = parquetFn
	f.Output = sqliteFn
	f.SQLTable = "report_tbl"
	f.PrepareOutput()

	proc, err := flatjsonl.NewProcessor(f, loadReportConfig(t), f.Inputs()...)
	require.NoError(t, err)
	require.NoError(t, proc.Process())

	rep := readRunReport(t, reportFn)
	assert.True(t, rep.Completed)
	require.Len(t, rep.Targets, 2)

	pq := findTarget(t, rep, parquetFn)
	assert.Equal(t, "parquet", pq.Type)
	assert.Equal(t, flatjsonl.TargetStatusReceivable, pq.Status)
	assert.Equal(t, int64(3), pq.RowsWritten)
	assert.NotEmpty(t, pq.Fields)
	assert.NotNil(t, pq.CompletedAt)
	require.Len(t, pq.Partitions, 1)
	assert.Equal(t, "primary", pq.Partitions[0].Kind)
	assert.Equal(t, int64(3), pq.Partitions[0].RowsWritten)
	assert.GreaterOrEqual(t, pq.Partitions[0].RowGroups, 1)

	sq := findTarget(t, rep, sqliteFn)
	assert.Equal(t, "sqlite", sq.Type)
	assert.Equal(t, flatjsonl.TargetStatusReceivable, sq.Status)
	assert.Equal(t, int64(3), sq.RowsWritten)
	assert.NotEmpty(t, sq.Fields)
	require.NotEmpty(t, sq.Partitions)
	assert.Equal(t, "report_tbl", sq.Partitions[0].Name)
	assert.Equal(t, int64(3), sq.Partitions[0].RowsWritten)
}

func TestRunReport_transposePartitions(t *testing.T) {
	reportFn := "testdata/transpose-report.json"
	parquetFn := "testdata/trans-report.parquet"

	require.NoError(t, os.RemoveAll(reportFn))

	extra := []string{
		parquetFn,
		"testdata/trans-report_tags.parquet",
		"testdata/trans-report_tokens.parquet",
		"testdata/trans-report_deep_arr.parquet",
		"testdata/trans-report_flat_map.parquet",
	}

	for _, fn := range extra {
		require.NoError(t, os.RemoveAll(fn))
		t.Cleanup(func(name string) func() {
			return func() { _ = os.Remove(name) }
		}(fn))
	}
	t.Cleanup(func() { _ = os.Remove(reportFn) })

	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.Input = "testdata/transpose.jsonl"
	f.Parquet = parquetFn
	f.ReplaceKeys = true
	f.Concurrency = 1
	f.Report = reportFn

	b, err := os.ReadFile("testdata/transpose_cfg.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config
	require.NoError(t, json.Unmarshal(b, &cfg))

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)
	require.NoError(t, proc.Process())

	rep := readRunReport(t, reportFn)
	require.True(t, rep.Completed)
	require.Len(t, rep.Targets, 1)

	pq := rep.Targets[0]
	// Primary plus one transposed partition per transpose destination.
	assert.GreaterOrEqual(t, len(pq.Partitions), 2)

	kinds := map[string]int64{}
	var transposeRows int64

	for _, pt := range pq.Partitions {
		kinds[pt.Kind]++
		if pt.Kind == "transpose" {
			transposeRows += pt.RowsWritten
			assert.NotEmpty(t, pt.Fields)
		}
	}

	assert.Equal(t, int64(1), kinds["primary"])
	assert.GreaterOrEqual(t, kinds["transpose"], int64(1))
	// Total rows include the transposed (exploded) rows.
	assert.Equal(t, pq.RowsWritten, pq.Partitions[0].RowsWritten+transposeRows)
}
