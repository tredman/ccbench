package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	mongoUrl            = flag.String("mongo_url", "localhost", "")
	testDBName          = flag.String("test_db_name", "test_db", "")
	controlDBName       = flag.String("control_db_name", "control_db", "")
	testDBNumDocs       = flag.Int64("test_db_num_docs", 1000000, "")
	testDBNumWorkers    = flag.Int64("test_db_num_workers", 10, "")
	testDBDocSize       = flag.Int64("test_db_doc_size", 512, "")
	testDBScanLimit     = flag.Int("test_db_scan_limit", 1000, "")
	controlDBNumDocs    = flag.Int64("control_db_num_docs", 1000000, "")
	controlDBNumWorkers = flag.Int64("control_db_num_workers", 10, "")
	controlDBDocSize    = flag.Int64("control_db_doc_size", 512, "")
	controlDBScanLimit  = flag.Int("control_db_scan_limit", 1000, "")
	rebuild             = flag.Bool("rebuid_dbs", false, "")
	runDuration         = flag.Duration("run_duration", time.Second*10, "run duration in seconds")
	verbose             = flag.Bool("verbose", false, "print stats each second")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	sess, err := mgo.Dial(*mongoUrl)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer sess.Close()

	if *rebuild {
		fmt.Printf("Building test db with %d documents of %d size.\n", *testDBNumDocs, *testDBDocSize)
		if err := initDB(sess, *testDBName, *testDBNumDocs, *testDBDocSize); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Printf("Building control db with %d documents of %d size.\n", *controlDBNumDocs, *controlDBDocSize)
		if err := initDB(sess, *controlDBName, *controlDBNumDocs, *controlDBDocSize); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	benchStats := NewBenchStats()

	for i := int64(0); i < *testDBNumWorkers; i++ {
		go runQueryWorker(benchStats, sess.Copy(), *testDBName, bson.M{"a": "1"}, *testDBScanLimit)
	}
	for i := int64(0); i < *controlDBNumWorkers; i++ {
		go runQueryWorker(benchStats, sess.Copy(), *controlDBName, bson.M{"a": "1"}, *controlDBScanLimit)
	}

	for i := int64(0); i < int64(*runDuration/time.Second); i++ {
		time.Sleep(time.Second)
		if *verbose {
			benchStats.PrintStats()
		}
		benchStats.Reset()
	}
	avgData := benchStats.GetHistoricalAvg()
	// Output is control worker count, control scan size, control doc size, test worker count, test scan size, test doc size
	// control ops/sec, control latency/op, test ops/sec, test latency/op
	fmt.Printf("%d, %d, %d, %d, %d, %d, %d, %d, %d, %d\n", *controlDBNumWorkers, *controlDBScanLimit, *controlDBDocSize,
		*testDBNumWorkers, *testDBScanLimit, *testDBDocSize, avgData[*controlDBName].OpCount, avgData[*controlDBName].LatencySum,
		avgData[*testDBName].OpCount, avgData[*testDBName].LatencySum)
}

func initDB(sess *mgo.Session, name string, numDocs int64, docSize int64) error {
	db := sess.DB(name)
	if err := db.DropDatabase(); err != nil {
		return fmt.Errorf("Error dropping db %s: %s", name, err)
	}
	col := db.C(name)
	doc := bson.M{"a": make([]byte, docSize-1)}
	for i := int64(0); i < numDocs; i++ {
		if err := col.Insert(doc); err != nil {
			return fmt.Errorf("Error inserting doc into DB: %s", err)
		}
	}

	return nil
}

func runQueryWorker(stat *BenchStats, sess *mgo.Session, db string, query bson.M, scanLimit int) {
	col := sess.DB(db).C(db)
	result := bson.M{}
	for {
		startTime := time.Now()
		col.Find(query).SetMaxScan(scanLimit).One(&result)
		duration := time.Since(startTime)
		stat.LogStat(db, duration)
	}
}

type DBStat struct {
	LatencySum time.Duration
	OpCount    int64
}

type BenchStats struct {
	mutex      sync.RWMutex
	stats      map[string]DBStat
	historical map[string][]DBStat
}

func NewBenchStats() *BenchStats {
	return &BenchStats{
		mutex:      sync.RWMutex{},
		stats:      map[string]DBStat{},
		historical: map[string][]DBStat{},
	}
}

func (b *BenchStats) LogStat(db string, latency time.Duration) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if _, ok := b.stats[db]; !ok {
		b.stats[db] = DBStat{}
	}
	stat := b.stats[db]
	stat.LatencySum += latency
	stat.OpCount += 1
	b.stats[db] = stat
}

func (b *BenchStats) PrintStats() {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	t := time.Now()
	// Output is time, db name, number of ops, sum latency in microseconds, avg latency in microsecs per op
	for db := range b.stats {
		fmt.Printf("%s, %s, %d, %d, %d\n", t, db, b.stats[db].OpCount, b.stats[db].LatencySum/1000, (int64(b.stats[db].LatencySum) / b.stats[db].OpCount / 1000))
	}
}

func (b *BenchStats) GetHistoricalAvg() map[string]DBStat {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	avgData := map[string]DBStat{}
	for db := range b.historical {
		runTotalLatency := time.Duration(0)
		runTotalOps := int64(0)
		for i, _ := range b.historical[db] {
			runTotalLatency += b.historical[db][i].LatencySum
			runTotalOps += b.historical[db][i].OpCount
		}
		avgData[db] = DBStat{
			LatencySum: time.Duration(int64(runTotalLatency) / runTotalOps / 1000),
			OpCount:    runTotalOps / int64((len(b.historical[db]))),
		}
	}
	return avgData
}

func (b *BenchStats) Reset() {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for db := range b.stats {
		if _, ok := b.historical[db]; !ok {
			b.historical[db] = make([]DBStat, 0, int64(*runDuration/time.Second))
		}
		b.historical[db] = append(b.historical[db], b.stats[db])
	}
	b.stats = map[string]DBStat{}
}
