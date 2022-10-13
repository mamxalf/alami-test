package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	csvBeforeEod = "Before Eod.csv"
	totalWorker  = 8
	csvAfterEod  = "After Eod.csv"
)

type beforeEod struct {
	id               int
	name             string
	age              int
	balanced         int
	previousBalanced int
	averageBalanced  int
	freeTransfer     int
}

func main() {
	start := time.Now()
	csvReader, csvReaderFile, err := openCsvFile()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer csvReaderFile.Close()

	jobs := make(chan beforeEod, 0)
	wg := new(sync.WaitGroup)

	csvWriter, csvWriterFile, err := writeCsvFile()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer csvWriterFile.Close()

	go dispatchWorkers(jobs, wg, csvWriter)
	readCsvFilePerLineThenSendToWorker(csvReader, jobs, wg)
	defer csvWriter.Flush()

	wg.Wait()

	// stopwatch
	duration := time.Since(start)
	fmt.Println("done in", int(math.Ceil(duration.Seconds())), "seconds")
}

func openCsvFile() (*csv.Reader, *os.File, error) {
	log.Println("=> open csv file")

	file, err := os.Open(csvBeforeEod)
	if err != nil {
		if os.IsNotExist(err) {
			log.Fatal("file unknown!")
		}

		return nil, nil, err
	}

	reader := csv.NewReader(file)
	reader.Comma = ';'
	return reader, file, nil
}

func writeCsvFile() (*csv.Writer, *os.File, error) {
	log.Println("=> write csv file")
	file, err := os.Create(csvAfterEod)
	if err != nil {
		log.Panic(err)
	}
	writer := csv.NewWriter(file)
	header := []string{"id", "Nama", "Age", "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced", "No 1 Thread-No", "Free Transfer", "No 2a Thread-No"}
	if err := writer.Write(header); err != nil {
		log.Fatalln("error writing header to file", err)
	}
	return writer, file, nil
}

func dispatchWorkers(jobs <-chan beforeEod, wg *sync.WaitGroup, csvWriter *csv.Writer) {
	for workerIndex := 0; workerIndex <= totalWorker; workerIndex++ {
		go func(workerIndex int, jobs <-chan beforeEod, wg *sync.WaitGroup, csvWriter *csv.Writer) {
			for job := range jobs {
				doTheJob(workerIndex, job, csvWriter)
				wg.Done()
			}
		}(workerIndex, jobs, wg, csvWriter)
	}
}

func doTheJob(workerIndex int, values beforeEod, csvWriter *csv.Writer) {
	for {
		var outerError error
		func(outerError *error) {
			defer func() {
				if err := recover(); err != nil {
					*outerError = fmt.Errorf("%v", err)
				}
			}()

			avgBalance := averageBalance(values.balanced, values.previousBalanced)
			freeTrf := freeTransferQuota(values.balanced, values.freeTransfer)
			addBalance := additionalBalance(values.id, values.balanced)
			log.Println("value: ", values, " average balanced: ", avgBalance, " free transfer: ", freeTrf, " pid: ", workerIndex, " final balanced : ", addBalance)
			row := []string{strconv.Itoa(values.id), values.name, strconv.Itoa(values.age), strconv.Itoa(addBalance), strconv.Itoa(workerIndex), strconv.Itoa(workerIndex), strconv.Itoa(values.previousBalanced), strconv.Itoa(avgBalance), strconv.Itoa(workerIndex), strconv.Itoa(freeTrf), strconv.Itoa(workerIndex)}
			if err := csvWriter.Write(row); err != nil {
				log.Fatalln("error writing row to file", err)
			}
			//defer csvWriter.Flush()

		}(&outerError)
		if outerError == nil {
			break
		}
	}
}

func readCsvFilePerLineThenSendToWorker(csvReader *csv.Reader, jobs chan<- beforeEod, wg *sync.WaitGroup) {
	isHeader := true
	for {
		row, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		if isHeader {
			isHeader = false
			continue
		}

		id, _ := strconv.Atoi(row[0])
		name := row[1]
		age, _ := strconv.Atoi(row[2])
		balanced, _ := strconv.Atoi(row[3])
		previousBalanced, _ := strconv.Atoi(row[4])
		averageBalanced, _ := strconv.Atoi(row[5])
		freeTransfer, _ := strconv.Atoi(row[6])

		rowResult := beforeEod{
			id:               id,
			name:             name,
			age:              age,
			balanced:         balanced,
			previousBalanced: previousBalanced,
			averageBalanced:  averageBalanced,
			freeTransfer:     freeTransfer,
		}

		wg.Add(1)
		jobs <- rowResult
	}
	close(jobs)
}

func averageBalance(balanced int, prevBalanced int) int {
	return (balanced + prevBalanced) / 2
}

func freeTransferQuota(balanced int, freeTransfer int) int {
	if balanced > 150 {
		return 25 + freeTransfer
	} else {
		return 5 + freeTransfer
	}
}

func additionalBalance(id int, balanced int) int {
	if id <= 100 {
		return balanced + 10
	}
	return balanced
}
