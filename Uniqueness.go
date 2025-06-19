Import (

"fmt"

"log"

"regexp"

"strings"

"sync"

"time"

"github.com/cis-ads-analytics-linking/pkg/constant"

"github.com/cis-ads-analytics-linking/pkg/csv"

"github.com/cis-ads-analytics-linking/pkg/report"

)

type SimpleRecord struct {

	refCount int64
	
	Rows int64
	
	Arrs [][] string
	
	}

	type Dataset struct {
		Records map[int] *SimpleRecord
		Rows int
		Columns int
		Header [] string
		Schema *arrow.Schema
	}
type mycc struct {

cv map[string]int

}

type colcount struct {

colvals []string //column array

piiresult []mattrs

}

type piicheck struct {

index int

piiattr string

val string

key string

}

type mattrs struct {

attrs map[int]piicheck

}

type piires struct {

colsmismatchrow int

key string

coln []colCount

}

type uniqueResults struct (
	msgs []string
	isunique bool
	uidx int	
	uniqcount []int
	}
	
	type or struct {
		compRegex map[string] *regexp.Regexp
	}
	var gccr
	
func Unique(data *csv.DataSet, rpt *report.Report) int {
	
	ts: time.Now()
	
	var row0 []string = data.Records[1].Arrs[0]
	
	expectedCols:= len(row0)
	
	rpt.Set (constant.RptNumColumns, expectedCols)
	
	if expectedCols ==0{
		log.Println(" expecting atleast 2 columns")
		return constant.UnqMissing2cols
	}

	routines:= len(data.Records)
	
	uniquemsg := []string{}
	
	uidx := 0
	
	results:= make([]uniqueResults, routines)
	
	message:= make (chan uniqueResults)
	
	var wg sync.WaitGroup
	
	for i:= 0; i < routines; i++ {
	
	wg.Add(1)
	
	log.Printf("Initializing routine with partition number: %d", i+1)
	
	partData:= data.Records[i+1].Arrs
	
	go func() {
		defer wg.Done()
	
	log.Printf("Initializing routine number: %d", i)
	
	checkUniqueness(partData, expectedCols, data.Rows, i, message)
	}()
	}
	ucolsCount:= make([]int, expectedCols)
	
	var ucols []int
	
	for r, _:= range results {
	
	results[r] = <-message
	
	log.Printf("Received response from routine number: %d", r)
	
	for i, v:= range results[r].uniqCount {

		if len(ucolscount) >= i {
		
		ucolscount[i]=ucolsCount[i]+v
		
		} else {
		
		ucolsCount[i]=v
		}
	}
	}
		for ix:= 1; ix < expectedCols; ix++ {//ignore first key column
		
		uniqCnt:= ucolsCount[ix]
		
		uniqper := float64(uniqCnt) / float64(data.Rows)* float64(100)
		
		log.Printf("unique percentage %f for the column index %d", uniqper, ix)
		
		if uniqper >= constant.UniquenessThreshold {
		
		ucols = append(ucols, ix)
		
		uidx++
		
		msg:= fmt.Sprintf("col [%d] uniqueness check successful. found (%) %2.2f unique, threshold (%%) %2.2f. Sample values below:",ix, unigper, constant. Uniqueness Threshold)
		
		log.Println(msg)
		
		uniqueMsg = append(uniqueMsg, msg)
		
		}
		
		rpt.Set(constant.RptuniquenessErrorMsg, strings.Join(uniqueMsg, "\n"))
		
		msg:= "uniqueness check complete time"
		
		log.Printf(msg+" %s\n", time.Since(ts))
		
		rpt.Set(constant. RptUniquenessErrorMsg, fmt.Sprintf(msg+" %s\n", time.Since(ts)))
		
		if uidx >0 {
		
		rpt.Set(constant.RptUniqueColumns, ucols)
		
		return constant.FoundUniquevalues
		}
		return 0
		
		}
		
		func checkUniqueness(data [][]string, expectedCols int, totoalRows int, routineNum int, ch chan uniqueResults) (
			
			timeT3:= time.Now()
			
			result:= uniqueResults{}
			
			cc:= make([]mycc, expectedCols)
			
			for ix:= 1; ix < expectedCols; ix++ {//ignore first key column
			
			cc[ix].cv = make(map[string]int)
			
			for row_ind, row := range data {
			
			fields:= row
			
			for i, field := range fields {			
			fields[i]= strings.Trimspace(field)
			}
			if ix < len(fields) {
				val:= cc[ix].cv[fields[ix]]
				val++ //occurence count 
				cc[ix].cv[fields[ix]] = val
			    
			} else {
			
			log.Printf("Missing column value at Row %d and Column %d Partition Number %d", row ind, ix, routineNum)
			
			}
			
			}
			
			log.Printf("col count %d", expectedcols)
			
			log.Printf("Step1 in routine number: %d, time %s", routineNum, time. Since(timeT3))
			
			log.Println("Total number of rows", totoalRows)
			
			ucols:= make([]int, expectedCols)
			
			for ix:= 1; ix < expectedCols; ix++ {//ignore first key column
			
				uniqCount:=0
			
			for _,v := range cc[ix].cv { 
				if v = 1 { //found only once in the col
					uniqCnt++
				}
			}
			ucols [ix] = uniqCount
			
			}
			
			result.uniqCount = ucols
			
			
			log.Printf("Sending back response in routine number: %d", routineNum)
			
			ch<-result
		}
