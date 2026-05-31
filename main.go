package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"log"
	"net/http"
	"os"
	"strconv"
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
	Cron          string
	LastRunTime   time.Time
	NextExecution time.Time
	Type          string
	Payload       json.RawMessage
	Status        string
	EntryID       cron.EntryID
}

type createJobRequest struct {
	Name    string
	Cron    string
	Type    string
	Payload json.RawMessage
}

type deleteJobRequest struct {
	UID string
}

type updateJobRequest struct {
	UID     string
	Name    string
	Cron    string
	Payload json.RawMessage
	Status  string
}

type payloadHTTP struct {
	URL     string
	Method  string
	Timeout int
}

type jobExecution struct {
	UID        string
	JobID      string
	ExecutedAt time.Time
	Duration   time.Duration
	StatusCode int
	Success    bool
	Error      string
}

var jobs []job
var ch chan job
var mut sync.Mutex
var rdb *redis.Client
var ctx = context.Background()
var c = cron.New()
var maxExecutionHistory int64

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	maxHistory := os.Getenv("MAX_EXECUTION_HISTORY")
	if maxHistory == "" {
		maxExecutionHistory = 99
	} else {
		val, err := strconv.ParseInt(maxHistory, 10, 64)
		if err != nil {
			log.Printf("invalid MAX_EXECUTION_HISTORY, defaulting to 99")
			maxExecutionHistory = 99
		} else {
			maxExecutionHistory = val
		}
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	rdb = redis.NewClient(&redis.Options{
		Addr: redisAddr,
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
	http.HandleFunc("/executions/", getHistoryJob)
	log.Fatal(http.ListenAndServe("0.0.0.0:8000", nil))
}

func createJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var req createJobRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	schedule, err := cron.ParseStandard(req.Cron)

	if err != nil {
		http.Error(w, "invalid cron expression", http.StatusBadRequest)
		return
	}

	j := job{
		UID:           uuid.New().String(),
		Name:          req.Name,
		Cron:          req.Cron,
		LastRunTime:   time.Time{},
		NextExecution: schedule.Next(time.Now()),
		Type:          req.Type,
		Payload:       req.Payload,
		Status:        "active",
	}

	entryID, _ := c.AddFunc(req.Cron, func() { ch <- j })
	j.EntryID = entryID
	jobs = append(jobs, j)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(j)
	exportJobs()
	log.Printf("Job {%v}\n", j)
}

func getJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

func getHistoryJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	uid := r.URL.Path[len("/executions/"):]
	if uid == "" {
		http.Error(w, "missing job uid", http.StatusBadRequest)
		return
	}
	data, err := rdb.LRange(ctx, "executions:"+uid, 0, -1).Result()
	if err != nil {
		http.Error(w, "couldn't fetch executions", http.StatusInternalServerError)
		return
	}
	var executions []jobExecution

	for _, d := range data {
		var e jobExecution
		json.Unmarshal([]byte(d), &e)
		executions = append(executions, e)
	}
	json.NewEncoder(w).Encode(executions)
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
			c.Remove(jobs[job].EntryID)
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
				"Updating the job %s [Name: %s, Interval: %s, Status: %s]\n",
				req.UID, req.Name, req.Cron, req.Status,
			)
			found = true
			if req.Name != "" {
				jobs[job].Name = req.Name
			}
			if req.Cron != "" {
				c.Remove(jobs[job].EntryID)
				newEntryID, _ := c.AddFunc(req.Cron, func() { ch <- jobs[job] })
				jobs[job].EntryID = newEntryID
				jobs[job].Cron = req.Cron
				schedule, err := cron.ParseStandard(req.Cron)
				if err != nil {
					log.Printf("error parsing cron expression")
				}
				jobs[job].NextExecution = schedule.Next(time.Now())
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
		var toExecute job
		for job := range jobs {
			if jobs[job].UID == j.UID {
				toExecute = jobs[job]
				jobs[job].LastRunTime = time.Now()
				break
			}
		}
		mut.Unlock()
		exportJobs()
		switch toExecute.Type {
		case jobTypeHTTP:
			executeHTTP(toExecute, workerId)
		case jobTypeScript:
			//TODO: Add different job types
		default:
			log.Printf("%s job type does not exist", j.Type)
		}
	}
}

func initScheduler() {
	log.Printf("Initialized the scheduler\n")
	c.Start()
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

	for i := range jobs {
		if jobs[i].Status == "active" {
			job := jobs[i]
			entryID, _ := c.AddFunc(jobs[i].Cron, func() { ch <- job })
			jobs[i].EntryID = entryID
		}
	}

	log.Printf("Loaded %d jobs from Redis", len(jobs))
}

func executeHTTP(j job, workerId int) {
	var payload payloadHTTP

	if err := json.Unmarshal(j.Payload, &payload); err != nil {
		log.Printf("error decoding payload: %v", err)
		return
	}

	if payload.Timeout == 0 {
		payload.Timeout = 30
	}

	start := time.Now()
	req, err := http.NewRequest(payload.Method, payload.URL, nil)

	if err != nil {
		log.Printf("could not create the request: %v", err)
		return
	}

	client := &http.Client{
		Timeout: time.Duration(payload.Timeout) * time.Second,
	}
	resp, err := client.Do(req)

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			log.Printf("request %s timed out", payload.URL)
		} else {
			log.Printf("could not complete the request: %v", err)
		}
		return
	}

	duration := time.Since(start)

	execution := jobExecution{
		UID:        uuid.New().String(),
		JobID:      j.UID,
		ExecutedAt: start,
		Duration:   duration,
		StatusCode: resp.StatusCode,
		Success:    resp.StatusCode >= 200 && resp.StatusCode < 300,
	}

	data, _ := json.Marshal(execution)
	rdb.LPush(ctx, "executions:"+j.UID, data)
	rdb.LTrim(ctx, "executions:"+j.UID, 0, maxExecutionHistory)

	defer resp.Body.Close()
	log.Printf("[%d] HTTP job %s: %d in %v", workerId, j.Name, resp.StatusCode, duration)
}
