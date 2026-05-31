# Hermes

Hermes is a concurrent job scheduler built in Go that executes tasks on 
configurable cron intervals using a worker pool. Jobs and execution history 
are persisted in Redis.

## Features

- Cron-based job scheduling with standard cron expressions
- Concurrent execution via a configurable worker pool
- Redis-backed persistence across restarts for jobs
- Per-job execution history with configurable retention
- HTTP API
- Dockerized

## Architecture

Jobs are registered with the cron scheduler and dispatched to a buffered 
channel. A pool of workers consumes from the channel concurrently, executing 
jobs and persisting results to Redis. Cron entries are tracked by EntryID 
to ensure updates and deletions remove stale scheduler registrations from the channel

## API

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /job | Create a job |
| GET | /jobs | List all jobs |
| POST | /update | Update a job |
| POST | /delete | Delete a job |
| GET | /executions/uid | Get execution history for a job |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| REDIS_ADDR | — | Redis address |
| MAX_EXECUTION_HISTORY | 99 | Max executions stored per job |

## Running locally

docker compose up --build
