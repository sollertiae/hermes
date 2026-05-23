package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"log"
	"net/http"
	"sync"
	"time"
)

const (
	jobTypeHTTP   = "http"
	jobTypeScript = "script"
)

type job struct {
	UID           string
	Name          string
	Interval_s    int
	LastRunTime   time.Time
	NextExecution time.Time
	Type          string
	Payload       json.RawMessage
	Status        string
}

type createJobRequest struct {
	Name       string
	Interval_s int
	Type       string
	Payload    json.RawMessage
}

type deleteJobRequest struct {
	UID string
}

type updateJobRequest struct {
	UID        string
	Name       string
	Interval_s int
	Payload    json.RawMessage
	Status     string
}

type payloadHTTP struct {
	URL    string
	Method string
}

var jobs []job
var ch chan job
var mut sync.Mutex
var rdb *redis.Client
var ctx = context.Background()

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	initJobs()

	ch = make(chan job, 100)

	numWorkers := flag.Int("workers", 3, "number of workers")
	flag.Parse()

	go initScheduler()
	for i := 0; i < *numWorkers; i++ {
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
		Type:          req.Type,
		Payload:       req.Payload,
		Status:        "active",
	}

	jobs = append(jobs, j)
	ch <- j
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(j)
	exportJobs()
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

	exportJobs()
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
			if req.Payload != nil {
				jobs[job].Payload = req.Payload
			}
			break
		}
	}
	mut.Unlock()
	if !found {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	exportJobs()
	w.WriteHeader(http.StatusOK)
}

func initWorker(workerId int) {
	log.Printf("Worker Initialzied %d\n", workerId)

	for j := range ch {
		mut.Lock()
		for job := range jobs {
			if jobs[job].UID == j.UID {
				switch jobs[job].Type {
				case jobTypeHTTP:
					executeHTTP(jobs[job])
				case jobTypeScript:
					//TODO: Add different job types
				default:
					log.Printf("%s job type does not exist", j.Type)
				}
			}
		}
		mut.Unlock()
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

func exportJobs() {
	mut.Lock()
	data, _ := json.Marshal(jobs)
	mut.Unlock()
	rdb.Set(ctx, "jobs", data, 0)
}

func initJobs() {
	data, err := rdb.Get(ctx, "jobs").Bytes()
	if err != nil {
		log.Printf("No jobs found in redis")
		return
	}
	mut.Lock()
	json.Unmarshal(data, &jobs)
	mut.Unlock()
	log.Printf("Loaded %d jobs from Redis", len(jobs))
}

func executeHTTP(j job) {
	var payload payloadHTTP

	if err := json.Unmarshal(j.Payload, &payload); err != nil {
		log.Printf("error decoding payload: %v", err)
		return
	}
	start := time.Now()
	req, err := http.NewRequest(payload.Method, payload.URL, nil)

	if err != nil {
		log.Printf("could not create the request: %v", err)
		return
	}

	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		log.Printf("could not complete the request: %v", err)
		return
	}

	duration := time.Since(start)

	defer resp.Body.Close()
	log.Printf("HTTP job %s: %d in %v", j.Name, resp.StatusCode, duration)
}
