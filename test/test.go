// Responsible for setting up, run, gather results and tear down a test. It
// exposes the method test.Run(), which saves the test results in the Test
// struct or fails.
package test

import (
	crypto_rand "crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	math_rand "math/rand"
	"regexp"
	"time"

	"github.com/golang/glog"
	"github.com/gpestana/kapacitor-unit/io"
	"github.com/gpestana/kapacitor-unit/task"
	"github.com/influxdata/telegraf"
	parser "github.com/influxdata/telegraf/plugins/parsers/influx"
	serializer "github.com/influxdata/telegraf/plugins/serializers/influx"
)

type Test struct {
	Name       string
	TaskName   string        `yaml:"task_name,omitempty"`
	DataPeriod time.Duration `yaml:"data_period"`
	DataJitter time.Duration `yaml:"data_jitter"`
	Data       []string
	metrics    []telegraf.Metric
	RecID      string `yaml:"recording_id"`
	Expects    Result
	Result     Result
	Db         string
	Rp         string
	Type       string
	Task       task.Task
}

func NewTest() Test {
	return Test{}
}

// Method exposed to start the test. It sets up the test, adds the test data,
// fetches the triggered alerts and saves it. It also removes all artifacts
// (database, retention policy) created for the test.
func (t *Test) Run(k io.Kapacitor, i io.Influxdb) error {

	defer t.teardown(k, i) //defer teardown so it gets run incase of early termination

	err := t.setup(k, i)
	if err != nil {
		return err
	}
	defer t.teardown(k, i)
	err = t.parseData()
	if err != nil {
		return err
	}
	err = t.addData(k, i)
	if err != nil {
		return err
	}
	t.wait()
	return t.results(k)
}

type Counter struct {
	end    time.Time
	step   time.Duration
	jitter time.Duration
	index  int
	total  int
}

func (c *Counter) increment() time.Time {
	i := int64(c.total-c.index) * c.step.Nanoseconds() * -1
	d := c.end.Add(time.Duration(i))
	// Add (or subtract) random jitter
	if c.jitter != time.Duration(0) {
		j := rand.Int63n(c.jitter.Nanoseconds())
		if rand.Intn(2) == 1 {
			j = -1 * j
		}
		d = d.Add(time.Duration(j))
	}
	c.index++
	return d
}

func (t *Test) parseData() error {
	if t.DataPeriod < t.DataJitter {
		return fmt.Errorf("data period should be greater than jitter")
	}
	handler := parser.NewMetricHandler()
	handler.SetTimePrecision(time.Nanosecond)
	var b [8]byte
	_, err := crypto_rand.Read(b[:])
	if err != nil {
		panic("cannot seed math/rand package with cryptographically secure random number generator")
	}
	math_rand.Seed(int64(binary.LittleEndian.Uint64(b[:])))
	c := &Counter{
		end:    time.Now(),
		index:  0,
		total:  len(t.Data),
		step:   t.DataPeriod,
		jitter: t.DataJitter,
	}
	handler.SetTimeFunc(c.increment)
	p := parser.NewParser(handler)
	t.metrics = make([]telegraf.Metric, len(t.Data))
	for i, d := range t.Data {
		m, err := p.ParseLine(d)
		if err != nil {
			return err
		}
		value, _ := m.GetField("temperature")
		log.Printf("%v %v", m.Time(), value)
		t.metrics[i] = m
	}
	return nil
}

func (t Test) String() string {
	if t.Result.Error == true {
		return fmt.Sprintf("TEST %v (%v) ERROR: %v", t.Name, t.TaskName, t.Result.String())
	} else {
		return fmt.Sprintf("TEST %v (%v) %v", t.Name, t.TaskName, t.Result.String())
	}
}

// Adds test data
func (t *Test) addData(k io.Kapacitor, i io.Influxdb) error {
	s := serializer.NewSerializer()
	data := make([][]byte, len(t.metrics))
	for i, m := range t.metrics {
		d, err := s.Serialize(m)
		if err != nil {
			return err
		}
		data[i] = d
	}
	switch t.Type {
	case "stream":
		// adds data to kapacitor
		err := k.Data(data, t.Db, t.Rp)
		if err != nil {
			return err
		}
	case "batch":
		// adds data to InfluxDb
		err := i.Data(data, t.Db, t.Rp)
		if err != nil {
			return err
		}
	}
	return nil
}

// Validates if individual test configuration is correct
func (t *Test) Validate() error {
	glog.Infof("DEBUG:: validate test: %s", t.Name)
	if len(t.Data) > 0 && t.RecID != "" {
		m := "Configuration file cannot define a recording_id and line protocol data input for the same test case"
		r := Result{0, 0, 0, m, false, true}
		t.Result = r
	}
	return nil
}

// Creates all necessary artifacts in database to run the test
func (t *Test) setup(k io.Kapacitor, i io.Influxdb) error {
	glog.Info("DEBUG:: setup test: ", t.Name)

	// Loads test task to kapacitor
	f := map[string]interface{}{
		"id":     t.TaskName,
		"type":   t.Type,
		"script": t.Task.Script,
		"status": "enabled",
	}

	if dbrp, _ := regexp.MatchString(`(?m:^dbrp \"\w+\"\.\"\w+\"$)`, t.Task.Script); !dbrp {
		f["dbrps"] = []map[string]string{{"db": t.Db, "rp": t.Rp}}
	}

	if err := k.Load(f); err != nil {
		return err
	}

	switch t.Type {
	case "batch":
		if err := i.Setup(t.Db, t.Rp); err != nil {
			return err
		}
	}
	return nil
}

func (t *Test) wait() {
	switch t.Type {
	case "batch":
		// If batch script, waits 3 seconds for batch queries being processed
		fmt.Println("Processing batch script " + t.TaskName + "...")
		time.Sleep(3 * time.Second)
	}
}

// Deletes data, database and retention policies created to run the test
func (t *Test) teardown(k io.Kapacitor, i io.Influxdb) {
	glog.Info("DEBUG:: teardown test: ", t.Name)
	switch t.Type {
	case "batch":
		err := i.CleanUp(t.Db)
		if err != nil {
			glog.Error("Error performing teardown in cleanup. error: ", err)
		}
	}
	err := k.Delete(t.TaskName)
	if err != nil {
		glog.Error("Error performing teardown in delete error: ", err)
	}

}

// Fetches status of kapacitor task, stores it and compares expected test result
// and actual result test
func (t *Test) results(k io.Kapacitor) error {
	s, err := k.Status(t.Task.Name)
	if err != nil {
		return err
	}
	t.Result = NewResult(s)
	t.Result.Compare(t.Expects)
	return nil
}
