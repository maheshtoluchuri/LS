package models

type PIIColumn struct {
	Column     []string
	ColInd     int
	ColumnData []string
}
type ColumnResults struct {
	Columns      map[int][]string
	RowPartition int
}
type SimpleRecord struct {
	refCount int64
	Rows     int64
	Arrs     [][]string
}
type Dataset struct {
	Records map[int]*SimpleRecord
	Rows    int
	Columns int
	Header  []string
	Schema  *arrow.Schema
}
