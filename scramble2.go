package common

import (
	"context"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/cis-ads-analytics-linking/pkg/csv"
)

func ShuffleRows(data *csv.DataSet) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recordChan := make(chan map[int]*csv.SimpleRecord, len(data.Records))
	resultChan := make(chan map[int]*csv.SimpleRecord, len(data.Records))

	var wg sync.WaitGroup
	var mu sync.Mutex

	// Use CPU cores as the basis for worker count
	numWorkers := runtime.NumCPU()

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go shuffleWorker(ctx, recordChan, resultChan, &wg)
	}
}

func deepCopyRecord(record *csv.SimpleRecord) *csv.SimpleRecord {
	recordCopy := &csv.SimpleRecord{
		refCount: record.refCount,
		Rows:     record.Rows,
		Arrs:     make([][]string, len(record.Arrs)),
	}

	// Deep copy each string slice to ensure complete isolation
	for i, arr := range record.Arrs {
		if arr != nil {
			recordCopy.Arrs[i] = make([]string, len(arr))
			copy(recordCopy.Arrs[i], arr)
		}
	}

	return recordCopy

	// Send work to workers with complete data isolation
	go func() {
		defer close(recordChan)

		mu.Lock()
		defer mu.Unlock()

		for key, record := range data.Records {
			// Create completely independent copy BEFORE sending to channel
			recordCopy := deepCopyRecord(record)
			recordMap := map[int]*csv.SimpleRecord{key: recordCopy}

			mu.Unlock() // Release lock before channel send to avoid blocking
			select {
			case <-ctx.Done():
				return
			case recordChan <- recordMap:
			}
			mu.Lock() // Re-acquire for next iteration
		}
	}()

	// Close result channel when all workers are done
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	for result := range resultChan {
		mu.Lock()
		for key, record := range result {
			data.Records[key] = record
		}
		mu.Unlock()
	}
}

func shuffleWorker(ctx context.Context, records <-chan map[int]*csv.SimpleRecord,
	results chan<- map[int]*csv.SimpleRecord, wg *sync.WaitGroup) {
	defer wg.Done()

	// Create a local random generator for this worker
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for recordMap := range records {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Data is already completely copied, so we can use it directly
		// No need for additional copying in the worker
		shuffleRecordArrays(recordMap, rng)

		select {
		case <-ctx.Done():
			return
		case results <- recordMap:
		}
	}
}

func shuffleRecordArrays(recordMap map[int]*csv.SimpleRecord, rng *rand.Rand) {
	for _, record := range recordMap {
		if len(record.Arrs) <= 1 {
			continue
		}

		// Fisher-Yates shuffle algorithm for better randomization
		for i := range record.Arrs {
			j := rng.Intn(len(record.Arrs))
			if i != j {
				record.Arrs[i], record.Arrs[j] = record.Arrs[j], record.Arrs[i]
			}
		}
	}
}
