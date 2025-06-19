package common

import (
	"context"
	"math/rand"
	"strings"
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
	var once sync.Once

	numworkers := len(data.Records) // <--
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go Shuffleworker(ctx, recordChan, resultChan, &wg)
	}
	var mu sync.Mutex
	go func() {
		for key, record := range data.Records {
			recordMap := make(map[int]*csv.SimpleRecord)
			mu.Lock()
			recordMap[key] = record
			mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case recordChan <- recordMap:
			}
		}
		close(recordChan)
	}()
	go func() {
		wg.Wait()
		close(resultChan)
	}()
	for {
		select {
		case res, ok := <-resultChan:
			if !ok {
				resultChan = nil
			} else {
				mu.Lock()
				for key, record := range res {
					data.Records[key] = record
				}
				mu.Unlock()
			}
		}
		if resultChan == nil {
			once.Do(func() {
				cancel()
			})
			break
		}
	}
}

func Shuffleworker(ctx context.Context, records <-chan map[int]*csv.SimpleRecord,
	results chan<- map[int]*csv.SimpleRecord, wg *sync.WaitGroup) {
	defer wg.Done()
	rand.New(rand.NewSource(time.Now().UnixNano()))

	for recordMap := range records {
		select {
		case <-ctx.Done():
			return
		default:
		}
		//deep copy the map and it's contents
		copiedMap := make(map[int]*csv.SimpleRecord)
		for k, v := range recordMap {
			copyRecord := *v
			copyRecord.Arrs = make([][]string, len(v.Arrs))
			for i, inner := range v.Arrs {
				CopyRecord.Arrs[i] = append([]string(nil), inner...)
			}
			copiedMap[k] = &copyRecord
		}

		for _, record := range copiedMap {
			for i, _ := range record.Arrs {
				r := rand.Intn(len(record.Arrs) - 1)
				if i != r {
					record.Arrs[i], record.Arrs[r] = record.Arrs[r], record.Arrs[i]
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case results <- copiedMap:
		}
		//results <- copiedMap
	}
}

func checkLeadingZero(key string) string {
	replacekey := ""
	ln := len(key)
	if ln < 9 {
		zeroCount := 9 - ln
		for j := 0; j < zeroCount; j++ {
			if j == zeroCount-1 {
				replacekey = replacekey + "0" + key
				key = replaceKey
			} else {
				replacekey += "0"
			}
		}
	}
	if replacekey != "" {
		return replaceKey
	}
	return key
}

func reassignkeyDigits(keyColumn string) string {
	strArr := strings.Split(keyColumn, "")
	for j = 0; j < len(strArr); j++ {
		switch strArr[j] {
		case "0":
			strArr[j] = "1"
		case "1":
			strArr[j] = "8"
		case "2":
			strArr[j] = "0"
		case "3":
			strArr[j] = "4"
		case "4":
			strArr[j] = "2"
		case "5":
			strArr[j] = "9"
		case "6":
			strArr[j] = "6"
		case "7":
			strArr[j] = "5"
		case "8":
			strArr[j] = "7"
		case "9":
			strArr[j] = "3"
		}
	}
	keyColumn = strings.Join(strArr, "")
	return keyColumn
}

func scrambleKeyPositions(reassignedkeys string) string {

	var tempArr [9]string

	key := strings.Split(reassignedKeys, "")

	tempArr[6] = key[0]

	tempArr[0] = key[1]

	tempArr[7] = key[2]

	tempArr[2] = key[3]

	tempArr[8] = key[4]

	tempArr[4] = key[5]

	tempArr[3] = key[6]

	tempArr[1] = key[7]

	tempArr[5] = key[8]
	tempkey := strings.Join(tempArr[:], "")
	return tempKey
}
