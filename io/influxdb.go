package io

import (
	"bytes"
	"fmt"
	"net/http"

	"github.com/golang/glog"
)

// Influxdb service configurations
type Influxdb struct {
	Host   string
	Client http.Client
}

func NewInfluxdb(host string) Influxdb {
	return Influxdb{
		host,
		http.Client{},
	}
}

// Adds test data to influxdb
func (influxdb Influxdb) Data(data [][]byte, db string, rp string) error {
	url := fmt.Sprintf("%s%sdb=%s&rp=%s", influxdb.Host, influxdb_write, db, rp)
	for _, d := range data {
		_, err := influxdb.Client.Post(url, "text/plain; charset=utf-8",
			bytes.NewBuffer(d))
		if err != nil {
			return err
		}
		glog.Infof("DEBUG:: Influxdb added %s to %s", string(d), url)
	}
	return nil
}

// Creates db and rp where tests will run
func (influxdb Influxdb) Setup(db string, rp string) error {
	glog.Infof("DEGUB:: Influxdb setup %s:%s", db, rp)
	// If no retention policy is defined, use "autogen"
	if rp == "" {
		rp = "autogen"
	}
	q := fmt.Sprintf("q=CREATE DATABASE \"%s\" WITH DURATION 1h REPLICATION 1 NAME \"%s\"", db, rp)
	baseURL := fmt.Sprintf("%s/query", influxdb.Host)
	_, err := influxdb.Client.Post(baseURL, "application/x-www-form-urlencoded",
		bytes.NewBuffer([]byte(q)))
	return err
}

func (influxdb Influxdb) CleanUp(db string) error {
	q := fmt.Sprintf("q=DROP DATABASE \"%s\"", db)
	glog.Info("DEBUG:: Influxdb cleanup database ", q)
	baseURL := fmt.Sprintf("%s/query", influxdb.Host)
	_, err := influxdb.Client.Post(baseURL, "application/x-www-form-urlencoded",
		bytes.NewBuffer([]byte(q)))
	return err
}
