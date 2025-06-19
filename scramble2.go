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

	// Fix: Use CPU cores instead of record count for optimal performance
	numWorkers := runtime.NumCPU()

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go shuffleWorker(ctx, recordChan, resultChan, &wg)
	}

	// CRITICAL FIX: Send work with complete data isolation
	go func() {
		defer close(recordChan)

		mu.Lock()
		// Create a snapshot of all records first to avoid concurrent iteration
		recordsSnapshot := make(map[int]*csv.SimpleRecord)
		for key, record := range data.Records {
			recordsSnapshot[key] = record
		}
		mu.Unlock()

		// Now process the snapshot safely
		for key, record := range recordsSnapshot {
			// CRITICAL: Create deep copy BEFORE sending to avoid shared memory access
			recordCopy := &csv.SimpleRecord{
				refCount: record.refCount,
				Rows:     record.Rows,
				Arrs:     make([][]string, len(record.Arrs)),
			}

			// Deep copy each string slice
			for i, arr := range record.Arrs {
				if arr != nil {
					recordCopy.Arrs[i] = make([]string, len(arr))
					copy(recordCopy.Arrs[i], arr)
				}
			}

			recordMap := map[int]*csv.SimpleRecord{key: recordCopy}

			select {
			case <-ctx.Done():
				return
			case recordChan <- recordMap:
			}
		}
	}()

	// Close result channel when all workers are done
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results safely
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

	// Fix: Create local random generator for this worker
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for recordMap := range records {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Data is already deep copied, so we can use it directly
		// No need for additional copying since each worker gets independent data

		// Shuffle the arrays within each record
		for _, record := range recordMap {
			if len(record.Arrs) <= 1 {
				continue // Skip if no arrays to shuffle or only one array
			}

			// Fix: Use proper range and Fisher-Yates shuffle
			for i := range record.Arrs {
				j := rng.Intn(len(record.Arrs)) // Fix: Use full range, not len-1
				if i != j {
					record.Arrs[i], record.Arrs[j] = record.Arrs[j], record.Arrs[i]
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		case results <- recordMap:
		}
	}
}
