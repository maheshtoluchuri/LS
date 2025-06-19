package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"
	"regexp"
	"slices"
	"strconv"
	"sync"

	"github.com/cis-ads-analytics-linking/pkg/config"
	"github.com/cis-ads-analytics-linking/pkg/constant"
	"github.com/cis-ads-analytics-linking/pkg/csv"
	"github.com/cis-ads-analytics-linking/pkg/mlmodel"
	models "github.com/cis-ads-analytics-linking/pkg/struct"
	"gonum.org/v1/gonum/mat"
)

//go:embed model_weights.gob
var modelWeightsGob []byte

// CompiledPIIFormat holds compiled regex patterns for better performance
type CompiledPIIFormat struct {
	Name  string
	Regex *regexp.Regexp
}

func PII(data *csv.DataSet, hashKeys []int) map[int][]models.PIIColumn {
	log.Printf("Starting PII detection process")

	// Validate input
	if data == nil || len(data.Records) == 0 {
		log.Printf("No data provided or empty dataset")
		return make(map[int][]models.PIIColumn)
	}

	// Decode model weights
	d := []float64{}
	decoder := gob.NewDecoder(bytes.NewReader(modelWeightsGob))
	if err := decoder.Decode(&d); err != nil {
		log.Printf("Error decoding model weights: %v", err)
		return nil
	}
	log.Printf("Loading weight model was successful")

	modelWeights := mat.NewVecDense(len(d), d)
	piiColumns := make(map[int][]models.PIIColumn)

	// Get percentage limit from environment
	percentage := os.Getenv(constant.PERCENT_CHECK)
	if percentage == "" {
		percentage = "100" // Default to 100% if not set
	}

	percent, err := strconv.Atoi(percentage)
	if err != nil {
		log.Printf("Error converting percentage from string to int: %v", err)
		return nil
	}

	// Calculate data limit
	limit := (data.Rows * percent) / 100
	log.Printf("Total dataset rows count: %d", data.Rows)
	log.Printf("PII data limit percent: %d", percent)
	log.Printf("PII data limit count: %d", limit)

	// Select limited data partitions
	limitCount := 0
	var limitedDataPartIndices []int

	for partitionIdx, record := range data.Records {
		if limitCount >= limit {
			break
		}
		limitCount += int(record.Rows)
		limitedDataPartIndices = append(limitedDataPartIndices, partitionIdx)
	}

	log.Printf("Limit data Record count %d", limitCount)

	// Process data partitions in parallel
	routines := len(limitedDataPartIndices)
	if routines == 0 {
		return piiColumns
	}

	results := make(chan models.ColumnResults, routines)
	columns := make(map[int][]string)

	var wg sync.WaitGroup

	// Start goroutines to join columns
	for _, partitionIdx := range limitedDataPartIndices {
		record := data.Records[partitionIdx]
		wg.Add(1)
		go func(pIdx int, rec *csv.SimpleRecord) {
			defer wg.Done()
			joinCols(pIdx, rec, results)
		}(partitionIdx, record)
	}

	// Wait for all goroutines to complete and close results channel
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect column results
	for columnResults := range results {
		for colIdx, values := range columnResults.Columns {
			columns[colIdx] = append(columns[colIdx], values...)
		}
	}

	// Pre-compile PII regex patterns for better performance
	piiFormats := config.PIIFormat()
	compiledPatterns := make([]CompiledPIIFormat, len(piiFormats))
	for i, pii := range piiFormats {
		compiledRegex, err := regexp.Compile(pii.Regex)
		if err != nil {
			log.Printf("Error compiling regex for %s: %v", pii.Name, err)
			continue
		}
		compiledPatterns[i] = CompiledPIIFormat{
			Name:  pii.Name,
			Regex: compiledRegex,
		}
	}

	// Detect PII columns in parallel
	piiColChan := make(chan models.PIIColumn, len(columns))
	var detectionWg sync.WaitGroup

	for colIdx, colValues := range columns {
		// Skip hash key columns
		if slices.Contains(hashKeys, colIdx) {
			continue
		}

		detectionWg.Add(1)
		go func(idx int, values []string) {
			defer detectionWg.Done()
			detectPIIColumns(idx, values, piiColChan, modelWeights, compiledPatterns)
		}(colIdx, colValues)
	}

	// Wait for all detection goroutines and close channel
	go func() {
		detectionWg.Wait()
		close(piiColChan)
	}()

	// Collect PII detection results
	for piiResult := range piiColChan {
		if len(piiResult.Column) == 0 {
			continue
		}

		if _, exists := piiColumns[piiResult.ColInd]; !exists {
			piiColumns[piiResult.ColInd] = []models.PIIColumn{}
		}
		piiColumns[piiResult.ColInd] = append(piiColumns[piiResult.ColInd], piiResult)
	}

	return piiColumns
}

// joinCols converts partition dataset into columnar format for checking column-wise PII data percentage
func joinCols(partition int, record *csv.SimpleRecord, ch chan models.ColumnResults) {
	col := models.ColumnResults{
		RowPartition: partition,
		Columns:      make(map[int][]string),
	}

	for _, rowValues := range record.Arrs {
		for colIdx, value := range rowValues {
			// Skip the first column (index 0) as per original logic
			if colIdx != 0 {
				col.Columns[colIdx] = append(col.Columns[colIdx], value)
			}
		}
	}

	ch <- col
}

// detectPIIColumns detects PII in a column using both regex patterns and ML model
func detectPIIColumns(colIdx int, colValues []string, ch chan models.PIIColumn, modelWeights *mat.VecDense, compiledPatterns []CompiledPIIFormat) {
	log.Printf("Doing PII detect for index: %d", colIdx)

	col := models.PIIColumn{
		ColInd:     colIdx,
		Column:     []string{},
		ColumnData: []string{},
	}

	piiCol := []string{}
	columnData := []string{}
	detectedTypes := make(map[string]bool) // To avoid duplicate PII type detection

	// Regex-based PII detection
	for _, value := range colValues {
		for _, compiledPattern := range compiledPatterns {
			if compiledPattern.Regex == nil {
				continue
			}

			if compiledPattern.Regex.MatchString(value) {
				log.Printf("Matching: %s, pattern: %s", value, compiledPattern.Name)

				// Add PII type if not already detected
				if !detectedTypes[compiledPattern.Name] {
					piiCol = append(piiCol, compiledPattern.Name)
					detectedTypes[compiledPattern.Name] = true
				}

				// Store column data if not hidden
				if os.Getenv(constant.HIDE_PII_DATA) != "true" {
					columnData = append(columnData, value)
				}
				break // Move to next value after first match
			}
		}
	}

	// ML-based name detection
	nameCount := 0
	for _, value := range colValues {
		if value == "" {
			continue // Skip empty values
		}

		features := mlmodel.FeatureHash(value)
		if len(features) == 0 {
			continue // Skip if no features extracted
		}

		featureVector := mat.NewVecDense(len(features), features)
		prediction := mlmodel.Predict(modelWeights, featureVector)

		if prediction > 0.8 {
			nameCount++
		}
	}

	// Check if column has significant percentage of names
	total := len(colValues)
	if total > 0 {
		namePercentage := float64(nameCount) / float64(total)
		if namePercentage >= 0.5 && !detectedTypes["NAME"] {
			piiCol = append(piiCol, "NAME")
		}
	}

	col.Column = piiCol
	col.ColumnData = columnData
	ch <- col
}
