package store

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/NimayPant/job-scheduler/pkg/models"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketJobs    = []byte("jobs")
	bucketTasks   = []byte("tasks")
	bucketWorkers = []byte("workers")
	bucketDAGs    = []byte("dags")
)

type BoltStore struct {
	db *bolt.DB
}

func NewBoltStore(path string) (*BoltStore, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("failed to open bolt db at %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketJobs, bucketTasks, bucketWorkers, bucketDAGs} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return fmt.Errorf("failed to create bucket %s: %w", string(b), err)
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &BoltStore{db: db}, nil
}

func (s *BoltStore) Close() error {
	return s.db.Close()
}

func (s *BoltStore) SaveJob(job *models.Job) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		data, err := json.Marshal(job)
		if err != nil {
			return fmt.Errorf("failed to marshal job %s: %w", job.ID, err)
		}
		if err := tx.Bucket(bucketJobs).Put([]byte(job.ID), data); err != nil {
			return fmt.Errorf("failed to put job %s: %w", job.ID, err)
		}
		// Persist tasks individually for lookup
		tb := tx.Bucket(bucketTasks)
		for _, t := range job.Tasks {
			td, err := json.Marshal(t)
			if err != nil {
				return fmt.Errorf("failed to marshal task %s: %w", t.ID, err)
			}
			if err := tb.Put([]byte(t.ID), td); err != nil {
				return fmt.Errorf("failed to put task %s: %w", t.ID, err)
			}
		}
		return nil
	})
}

func (s *BoltStore) GetJob(id string) (*models.Job, error) {
	var job models.Job
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketJobs).Get([]byte(id))
		if data == nil {
			return fmt.Errorf("job %s not found", id)
		}
		return json.Unmarshal(data, &job)
	})
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (s *BoltStore) ListJobs(filterState *models.JobState, limit int) ([]*models.Job, error) {
	var jobs []*models.Job
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketJobs)
		return b.ForEach(func(k, v []byte) error {
			if limit > 0 && len(jobs) >= limit {
				return nil
			}
			var job models.Job
			if err := json.Unmarshal(v, &job); err != nil {
				return err
			}
			if filterState != nil && job.State != *filterState {
				return nil
			}
			jobs = append(jobs, &job)
			return nil
		})
	})
	return jobs, err
}

func (s *BoltStore) DeleteJob(id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		jb := tx.Bucket(bucketJobs)
		data := jb.Get([]byte(id))
		if data == nil {
			return fmt.Errorf("job %s not found", id)
		}
		var job models.Job
		if err := json.Unmarshal(data, &job); err != nil {
			return err
		}
		tb := tx.Bucket(bucketTasks)
		for _, t := range job.Tasks {
			if err := tb.Delete([]byte(t.ID)); err != nil {
				return err
			}
		}
		return jb.Delete([]byte(id))
	})
}

func (s *BoltStore) GetTask(id string) (*models.Task, error) {
	var task models.Task
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketTasks).Get([]byte(id))
		if data == nil {
			return fmt.Errorf("task %s not found", id)
		}
		return json.Unmarshal(data, &task)
	})
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func (s *BoltStore) UpdateTask(task *models.Task) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		data, err := json.Marshal(task)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketTasks).Put([]byte(task.ID), data)
	})
}

func (s *BoltStore) SaveWorker(worker *WorkerRecord) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		data, err := json.Marshal(worker)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketWorkers).Put([]byte(worker.ID), data)
	})
}

func (s *BoltStore) GetWorker(id string) (*WorkerRecord, error) {
	var w WorkerRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketWorkers).Get([]byte(id))
		if data == nil {
			return fmt.Errorf("worker %s not found", id)
		}
		return json.Unmarshal(data, &w)
	})
	if err != nil {
		return nil, err
	}
	return &w, nil
}

func (s *BoltStore) ListWorkers() ([]*WorkerRecord, error) {
	var workers []*WorkerRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketWorkers).ForEach(func(k, v []byte) error {
			var w WorkerRecord
			if err := json.Unmarshal(v, &w); err != nil {
				return err
			}
			workers = append(workers, &w)
			return nil
		})
	})
	return workers, err
}

func (s *BoltStore) DeleteWorker(id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketWorkers).Delete([]byte(id))
	})
}

func (s *BoltStore) SaveDAG(dag *models.DAG) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		data, err := json.Marshal(dag)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketDAGs).Put([]byte(dag.ID), data)
	})
}

func (s *BoltStore) GetDAG(id string) (*models.DAG, error) {
	var dag models.DAG
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketDAGs).Get([]byte(id))
		if data == nil {
			return fmt.Errorf("dag %s not found", id)
		}
		return json.Unmarshal(data, &dag)
	})
	if err != nil {
		return nil, err
	}
	return &dag, nil
}

type WorkerRecord struct {
	ID              string                  `json:"id"`
	Address         string                  `json:"address"`
	State           models.WorkerState      `json:"state"`
	Resources       models.ResourceCapacity `json:"resources"`
	RunningTaskIDs  []string                `json:"running_task_ids"`
	LastHeartbeat   time.Time               `json:"last_heartbeat"`
}
