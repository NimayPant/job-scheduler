package scheduler

import (
	"fmt"

	"github.com/NimayPant/job-scheduler/pkg/models"
	"github.com/NimayPant/job-scheduler/pkg/store"
)

type PlacementStrategy int

const (
	StrategyBestFit PlacementStrategy = iota
	StrategySpread
)

type Placer struct {
	strategy PlacementStrategy
}

func NewPlacer(strategy PlacementStrategy) *Placer {
	return &Placer{strategy: strategy}
}

func (p *Placer) ScoreWorker(task *models.Task, worker *store.WorkerRecord) float64 {
	if worker.State != models.WorkerStateActive {
		return 0
	}
	if !worker.Resources.Fits(task.ResourceReqs) {
		return 0
	}

	switch p.strategy {
	case StrategyBestFit:
		return p.scoreBestFit(task, worker)
	case StrategySpread:
		return p.scoreSpread(task, worker)
	default:
		return p.scoreBestFit(task, worker)
	}
}

func (p *Placer) scoreBestFit(task *models.Task, worker *store.WorkerRecord) float64 {
	avail := worker.Resources.Available
	req := task.ResourceReqs

	var dimensions, score float64

	if avail.CPUCores > 0 {
		dimensions++
		score += float64(req.CPUCores) / float64(avail.CPUCores)
	}
	if avail.MemoryMB > 0 {
		dimensions++
		score += float64(req.MemoryMB) / float64(avail.MemoryMB)
	}
	if avail.GPUs > 0 && req.GPUs > 0 {
		dimensions++
		score += float64(req.GPUs) / float64(avail.GPUs)
	}
	if avail.DiskMB > 0 && req.DiskMB > 0 {
		dimensions++
		score += float64(req.DiskMB) / float64(avail.DiskMB)
	}

	if dimensions == 0 {
		return 0.5 // No resource constraints, neutral score
	}
	return score / dimensions
}

func (p *Placer) scoreSpread(task *models.Task, worker *store.WorkerRecord) float64 {
	total := worker.Resources.Total
	avail := worker.Resources.Available
	req := task.ResourceReqs

	var dimensions, score float64

	if total.CPUCores > 0 {
		dimensions++
		remaining := float64(avail.CPUCores-req.CPUCores) / float64(total.CPUCores)
		score += remaining
	}
	if total.MemoryMB > 0 {
		dimensions++
		remaining := float64(avail.MemoryMB-req.MemoryMB) / float64(total.MemoryMB)
		score += remaining
	}
	if total.GPUs > 0 && req.GPUs > 0 {
		dimensions++
		remaining := float64(avail.GPUs-req.GPUs) / float64(total.GPUs)
		score += remaining
	}
	if total.DiskMB > 0 && req.DiskMB > 0 {
		dimensions++
		remaining := float64(avail.DiskMB-req.DiskMB) / float64(total.DiskMB)
		score += remaining
	}

	if dimensions == 0 {
		return 0.5
	}
	return score / dimensions
}

func (p *Placer) SelectWorker(task *models.Task, workers []*store.WorkerRecord) (*store.WorkerRecord, float64, error) {
	var bestWorker *store.WorkerRecord
	bestScore := -1.0

	for _, w := range workers {
		score := p.ScoreWorker(task, w)
		if score <= 0 {
			continue
		}
		if score > bestScore {
			bestScore = score
			bestWorker = w
		}
	}

	if bestWorker == nil {
		return nil, 0, fmt.Errorf("no eligible worker for task %s (requires %s)", task.ID, task.ResourceReqs)
	}
	return bestWorker, bestScore, nil
}

func EstimateClusterCapacity(workers []*store.WorkerRecord) (total, available models.ResourceRequirements) {
	for _, w := range workers {
		if w.State != models.WorkerStateActive {
			continue
		}
		total.CPUCores += w.Resources.Total.CPUCores
		total.MemoryMB += w.Resources.Total.MemoryMB
		total.GPUs += w.Resources.Total.GPUs
		total.DiskMB += w.Resources.Total.DiskMB

		available.CPUCores += w.Resources.Available.CPUCores
		available.MemoryMB += w.Resources.Available.MemoryMB
		available.GPUs += w.Resources.Available.GPUs
		available.DiskMB += w.Resources.Available.DiskMB
	}
	return
}

func CanFitAnywhere(task *models.Task, workers []*store.WorkerRecord) bool {
	for _, w := range workers {
		if w.State == models.WorkerStateActive && w.Resources.Fits(task.ResourceReqs) {
			return true
		}
	}
	return false
}
