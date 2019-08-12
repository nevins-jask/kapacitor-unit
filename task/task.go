// Keep data about a task to be tested and interface to run all task's tests
package task

import (
	"io/ioutil"
	"path"
)

// FS configurations, namely path where TICKscripts are located
type Task struct {
	Name   string
	Path   string
	Script string
}

// Task constructor
func New(n string, p string) (*Task, error) {
	task := Task{
		Name: n,
		Path: p,
	}

	scriptPath := path.Join(p, n)
	s, err := ioutil.ReadFile(scriptPath)
	if err != nil {
		return nil, err
	}
	task.Script = string(s[:])
	return &task, nil
}
