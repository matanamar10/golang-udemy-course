package main

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Job represents a unit of work.
type Job struct {
	ID     int
	Status string
}

// ProcessJob simulates job processing and returns an error if something goes wrong.
func ProcessJob(job *Job) error {
	// Simulate a job that fails if the ID is odd
	if job.ID%2 != 0 {
		return errors.New(fmt.Sprintf("Job %d failed", job.ID))
	}
	job.Status = "Completed"
	return nil
}

// jobProcessorFactory returns a function that processes jobs and uses a mutex to safely update the status.
func jobProcessorFactory(mutex *sync.Mutex) func(*Job) error {
	return func(job *Job) error {
		mutex.Lock()
		defer mutex.Unlock()

		fmt.Printf("Processing job %d\n", job.ID)
		err := ProcessJob(job)
		if err != nil {
			fmt.Println("Error:", err)
			job.Status = "Failed"
			return err
		}

		fmt.Printf("Job %d completed successfully\n", job.ID)
		return nil
	}
}

// worker is a goroutine that processes jobs from the job channel.
func worker(id int, jobs <-chan *Job, wg *sync.WaitGroup, processJob func(*Job) error, counter *int64) {
	defer wg.Done()

	for job := range jobs {
		fmt.Printf("Worker %d started job %d\n", id, job.ID)
		err := processJob(job)
		if err == nil {
			atomic.AddInt64(counter, 1) // Increment atomic counter if the job was successful
		}
		fmt.Printf("Worker %d finished job %d with status: %s\n", id, job.ID, job.Status)
		time.Sleep(time.Millisecond * 500) // Simulate time taken to process
	}
}

func main() {
	numJobs := 10
	numWorkers := 3

	// Create a channel to hold jobs and a WaitGroup to wait for all workers
	jobChannel := make(chan *Job, numJobs)
	var wg sync.WaitGroup
	var successfulJobs int64 // Atomic counter for successful jobs
	var mutex sync.Mutex     // Mutex for synchronizing job status updates

	// Create the job processor function with mutex protection
	processJob := jobProcessorFactory(&mutex)

	// Start workers as goroutines
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go worker(w, jobChannel, &wg, processJob, &successfulJobs)
	}

	// Send jobs to the job channel
	for j := 1; j <= numJobs; j++ {
		job := &Job{ID: j, Status: "Pending"} // Create new job
		jobChannel <- job                     // Send job to the channel
	}

	// Close the channel after all jobs are sent
	close(jobChannel)

	// Wait for all workers to finish
	wg.Wait()

	fmt.Printf("All jobs processed. Total successful jobs: %d out of %d\n", successfulJobs, numJobs)
}
