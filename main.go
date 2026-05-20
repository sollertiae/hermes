package main

import (
	"encoding/json"
	"github.com/google/uuid"
	"log"
	"net/http"
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

type deleteJobRequest struct {
	UID string
}

type updateJobRequest struct {
	UID        string
	Name       string
	Interval_s int
	Status     string
}

var jobs []job
var ch chan job
var mut sync.Mutex

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	ch = make(chan job, 100)
	go initScheduler()
	for i := 0; i < 3; i++ {
		go initWorker(i)
	}

	http.HandleFunc("/job", createJob)
	http.HandleFunc("/jobs", getJob)
	http.HandleFunc("/delete", deleteJob)
	http.HandleFunc("/update", updateJob)
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
	log.Printf("Job {%v}\n", j)
}

func getJob(w http.ResponseWriter, r *http.Request) {
	log.Printf("List of jobs:\n")
	for i, job := range jobs {
		log.Printf("[%d] - %v\n", i, job)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

func deleteJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var req deleteJobRequest
	found := false

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	mut.Lock()
	for job := range jobs {
		if jobs[job].UID == req.UID {
			found = true
			log.Printf("Removing the job %s\n", req.UID)
			jobs = append(jobs[:job], jobs[job+1:]...)
			break
		}
	}
	mut.Unlock()

	if !found {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func updateJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var req updateJobRequest
	found := false

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	mut.Lock()
	for job := range jobs {
		if jobs[job].UID == req.UID {
			log.Printf(
				"Updating the job %s [Name: %s, Interval: %d, Status: %s]\n",
				req.UID, req.Name, req.Interval_s, req.Status,
			)
			found = true
			if req.Name != "" {
				jobs[job].Name = req.Name
			}
			if req.Interval_s != 0 {
				jobs[job].Interval_s = req.Interval_s
				jobs[job].NextExecution = time.Now().Add(time.Duration(req.Interval_s) * time.Second)
			}
			if req.Status != "" {
				jobs[job].Status = req.Status
			}
			break
		}
	}
	mut.Unlock()
	if !found {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func initWorker(workerId int) {
	log.Printf("Worker Initialzied %d\n", workerId)

	for j := range ch {
		mut.Lock()
		for job := range jobs {
			if jobs[job].UID == j.UID {
				mut.Unlock()
				//TODO: Processing something
				log.Printf("job processing\n")
				time.Sleep(10 * time.Second)
				log.Printf("job processed\n")
				break
			}
		}
	}
}

func initScheduler() {
	log.Printf("Initialized the scheduler\n")
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		mut.Lock()
		for job := range jobs {
			if jobs[job].NextExecution.Before(time.Now()) {
				log.Printf(
					"Found job %s which needs to trigger at %v\n",
					jobs[job].Name, jobs[job].NextExecution,
				)

				jobs[job].NextExecution = time.Now().Add(
					time.Duration(jobs[job].Interval_s) * time.Second,
				)

				jobs[job].LastRunTime = time.Now()
				ch <- jobs[job]
			}
		}
		mut.Unlock()
	}
}
