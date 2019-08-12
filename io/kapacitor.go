package io

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"

	"github.com/golang/glog"
)

type Status struct {
	Data map[string]map[string]interface{} `json:"stats"`
}

// Kapacitor service configurations
type Kapacitor struct {
	Host   string
	Client http.Client
}

func NewKapacitor(host string) Kapacitor {
	return Kapacitor{
		host,
		http.Client{},
	}
}

// Loads a task
func (k Kapacitor) Load(f map[string]interface{}) error {
	glog.Infof("DEBUG:: Kapacitor loading task: %s", f["id"])
	// Replaces '.every()' if type of script is batch
	if f["type"] == "batch" {
		str, ok := f["script"].(string)
		if ok != true {
			return errors.New("Task Load: script is not of type string")
		}
		f["script"] = batchReplaceEvery(str)

		glog.Infof("DEBUG:: batch script after replace: %s", f["script"])
	}

	j, err := json.Marshal(f)
	if err != nil {
		return err
	}

	u := fmt.Sprintf("%s%s", k.Host, tasks)
	res, err := k.Client.Post(u, "application/json", bytes.NewBuffer(j))
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		r, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("%s::%s", res.Status, string(r))
	}
	return nil
}

// Deletes a task
func (k Kapacitor) Delete(id string) error {
	u := fmt.Sprintf("%s%s/%s", k.Host, tasks, id)
	r, err := http.NewRequest("DELETE", u, nil)
	if err != nil {
		return err
	}
	_, err = k.Client.Do(r)
	if err != nil {
		return err
	}
	glog.Infof("DEBUG:: Kapacitor deleted task: %s", id)
	return nil
}

// Adds test data to kapacitor
func (k Kapacitor) Data(data [][]byte, db string, rp string) error {
	u := fmt.Sprintf("%s%sdb=%s&rp=%s", k.Host, kapacitor_write, db, rp)
	for _, d := range data {
		_, err := k.Client.Post(u, "application/x-www-form-urlencoded",
			bytes.NewBuffer(d))
		if err != nil {
			return err
		}
		glog.Infof("DEBUG:: Kapacitor added data: %s", d)
	}
	return nil
}

// Gets task alert status
func (k Kapacitor) Status(id string) (map[string]int, error) {
	glog.Infof("DEBUG:: Kapacitor fetching status of: %s", id)
	u := fmt.Sprintf("%s%s/%s", k.Host, tasks, id)
	res, err := k.Client.Get(u)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	var s Status
	b, err := ioutil.ReadAll(res.Body)
	err = json.Unmarshal(b, &s)
	if err != nil {
		return nil, err
	}
	f := make(map[string]int)
	var sa interface{}
	for key, value := range s.Data["node-stats"] {
		if strings.HasPrefix(key, "alert") {
			sa = value
			for k, val := range sa.(map[string]interface{}) {
				switch v := val.(type) {
				case float64:
					f[k] += int(v)
				default:
					return nil, errors.New("kapacitor.status: wrong response from service")
				}
			}
		}
	}
	if sa == nil {
		return nil, errors.New("kapacitor.status: expected alert.* key to be found on stats")
	}
	return f, nil
}

// Replaces '.every(*)' for the batch request to be performed every 1s to speed up the test
func batchReplaceEvery(s string) string {
	re := regexp.MustCompile("every\\((.*?)\\)")
	return re.ReplaceAllString(s, "every(1s)")
}
