package main

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type job struct {
	UID           string
	Name          string
	Interval_s    int
	LastRunTime   time.Time
	NextExecution time.Time
	Status        string
}

type createJobRequest struct {
	Name       string
	Interval_s int
}

var jobs []job
var ch chan job
var mut sync.Mutex

func main() {
	ch = make(chan job, 100)
	go initScheduler()
	for i := 0; i < 3; i++ {
		go initWorker(i)
	}

	http.HandleFunc("/job", createJob)
	http.HandleFunc("/jobs", getJob)
	log.Fatal(http.ListenAndServe("localhost:8000", nil))
}

func createJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var req createJobRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	j := job{
		UID:           uuid.New().String(),
		Name:          req.Name,
		Interval_s:    req.Interval_s,
		LastRunTime:   time.Time{},
		NextExecution: time.Now().Add(time.Duration(req.Interval_s) * time.Second),
		Status:        "active",
	}

	jobs = append(jobs, j)
	ch <- j
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(j)
	fmt.Fprintf(os.Stdout, "[%s] Job {%v}\n", time.Now().Format("15:04:05.000"), j)
}

func getJob(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(os.Stdout, "List of jobs:\n")
	for i, job := range jobs {
		fmt.Fprintf(os.Stdout, "[%d] - %v\n", i, job)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

func initWorker(workerId int) {
	fmt.Fprintf(os.Stdout, "[%s] Worker Initialzied %d\n", time.Now().Format("15:04:05.000"), workerId)

	for j := range ch {
		mut.Lock()
		for job := range jobs {
			if jobs[job].UID == j.UID {
				mut.Unlock()
				//TODO: Processing something
				fmt.Printf("[%s] job processing\n", time.Now().Format("15:04:05.000"))
				time.Sleep(10 * time.Second)
				fmt.Printf("[%s] job processed\n", time.Now().Format("15:04:05.000"))
				break
			}
		}
	}
}

func initScheduler() {
	fmt.Fprintf(os.Stdout, "[%s] Initialized the scheduler\n", time.Now().Format("15:04:05.000"))
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		mut.Lock()
		for job := range jobs {
			if jobs[job].NextExecution.Before(time.Now()) {
				fmt.Fprintf(
					os.Stdout,
					"[%s] Found job %s which needs to trigger at %v\n",
					time.Now().Format("15:04:05.000"), jobs[job].Name, jobs[job].NextExecution
				)
				
				jobs[job].NextExecution = time.Now().Add(
					time.Duration(jobs[job].Interval_s) * time.Second
				)
				
				jobs[job].LastRunTime = time.Now()
				ch <- jobs[job]
			}
		}
		mut.Unlock()
	}
}
