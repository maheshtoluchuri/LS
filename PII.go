
import (

	"bytes"
	
	"embed"
	
	"encoding/gob"
	
	"fmt"
	
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
	
	//go: embed model weights.gob
	
	var modelweightsGob [] byte
	
	func PII(data *csv.DataSet, hash keys []int) map[int] [] models.PIIColumn { 
		log.Printf("Starting PII detection process")
	
	d:= [] float64{}
	
	decoder:= gob.NewDecoder(bytes. NewReader(modelweightsGob)) 
	
	
	if err := decoder. Decode(&d); err != nil {
		log.Printf("Error decoding model weights: %v\n", err)
	return nil
	
	}
	log.Printf("Loading weight model was successful")

mweight:= mat. NewVecDense(len(d), d)

//mlmodel, err models.ReadFile("mlmodel/model weights.gob")

piicolumns:= make(map[int] [] models.PIIColumn)

if len(data.Records) == 0 {

return piicolumns

}

//Fetch the specied percentage limit of data from dataset

percentage:= os.Getenv(constant.PERCENT_CHECK)

percent, err := strconv.Atoi (percentage)

if err != nil {

log.Printf("Error converting percentage from string to int: %v\n", err)

return nil

}

limit:= (data.Rows * percent) / 100

log.Printf("Total dataset rows count: %d", data.Rows)

log.Printf("PII data limit percent: %d", percent)

log.Printf("PII data limit count: %d", limit)

limitcnt:= 0

var limitedDataPartInd []int

for p, record := range data. Records {

//if the specifed precentage of records match stop columns joining for PII records checking

if limitcnt >= limit {

break

}

limitent = limitcnt + int(record.Rows)

limitedDataPartInd = append(limitedDataPartInd, p)
}
routines:= len(limitedDataPartInd)

results:= make(chan models.ColumnResults, routines)

columns:= make (map[int][] string)

log.Printf("Limit data Record count %d", limitcnt)

var wg sync.WaitGroup

for _,p := range limitedDataPartInd {

record:= data. Records[p]

wg.Add(1)

go func() {

defer wg. Done()

joincols (p, record, results)

}()

}

for _= range LimitedDataPartInd {

columnResults:= <-results

for colind, val range columnResults.columns {
	columns[colInd]= append(columns[colInd], val...)
}

}

piicolumnRes:= make([]models.PIIColumn, len(columns))

piicolchan:= make (chan models.PIIColumn)

for colIdx, colvalues:= range columns {

if slices.Contains(hash_keys, colIdx) {

continue

}

wg.Add(1)

go func() {
	defer wg. Done()
	detectPIIColumns(colidx, colvalues, piiColchan, mweight)
}()

}

for rind,_:= range piicolumnRes {

if slices.Contains(hash_keys, rind) {

continue

}

piicolumnRes[rind] =<-piicolchan

res:= piicolumnRes[rind]

if len(res.Column) == 0 {

continue

}

if exists: RiiColumns[res.Colind]; !exists {

piicolumns[res.ColInd] = []models.PIIColumn{}

}

piicolumns[res.colInd] append(piicolumns[res.Colind), res)
}

return piicolumns

}

/*

Covert partition dataset into columnar format for checking column wise PII data percentage

*/

func joinCols(partition int, record *csv.SimpleRecord, ch chan models.ColumnResults) {

col:= models.ColumnResults{}

cols:= make(map[int] [] string)

for _,rowvalues:= range record. Arrs {

for colIdx, value:= range rowValues {

if colIdx != 0 {

cols [colidx] = append(cols[colIdx], value)

}

}

}

col.RowPartition = partition

col.Columns = cols

ch <- col

} 
func detectPIIColumns (colIdx int, colvalues [] string, ch chan models.PIIColumn, modelweights *mat.VecDense) {

log.Printf("Doing PII detect for index: %d", colIdx)

piiFormats:= config.PIIFormat()

col:= models.PIIColumn{}

piicol := []string{}

columnData:= [] string{}

for, pii:= range piiFormats {

re:= regexp.MustCompile(pii.Regex)

for _,value:= range colvalues {
	regexp.MustCompile(p11.Regex)

for _, value:= range colvalues {

if re.MatchString(value) {

fmt.Printf("Matching: %s, %s", value, re)

piicol = append(piicol, pii.Name)

if os.Getenv(constant.HIDE_PII_DATA) == "false" {

columnData = append(columnData, value)

}

break

}

}

}

name:= 0

for _,value := range colvalues {

features := mlmodel.FeatureHash (value)

featurevector:= mat.NewVecDense (len(features), features)

if mlmodel.Predict(modelweights, featurevector) > 0.8 {

name++

}


total:= len(colvalues)

if total > 0 {

namePercentage:= float64(name) / float64(total)

if namePercentage >= 0.5 {

piicol= append(piiCol, "NAME")

}

}

col.Colind= colIdx

col.Column = piicol

col.ColumnData = columnData

ch <- col

}