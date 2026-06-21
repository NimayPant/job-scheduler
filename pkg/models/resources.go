package models

import "fmt"

type ResourceRequirements struct {
	CPUCores int   `json:"cpu_cores"`
	MemoryMB int64 `json:"memory_mb"`
	GPUs     int   `json:"gpus"`
	DiskMB   int64 `json:"disk_mb"`
}

func (r ResourceRequirements) IsZero() bool {
	return r.CPUCores == 0 && r.MemoryMB == 0 && r.GPUs == 0 && r.DiskMB == 0
}

func (r ResourceRequirements) String() string {
	return fmt.Sprintf("CPU:%d Mem:%dMB GPU:%d Disk:%dMB", r.CPUCores, r.MemoryMB, r.GPUs, r.DiskMB)
}

type ResourceCapacity struct {
	Total     ResourceRequirements `json:"total"`
	Available ResourceRequirements `json:"available"`
}

func NewResourceCapacity(total ResourceRequirements) ResourceCapacity {
	return ResourceCapacity{
		Total:     total,
		Available: total,
	}
}

func (c *ResourceCapacity) Fits(req ResourceRequirements) bool {
	return c.Available.CPUCores >= req.CPUCores &&
		c.Available.MemoryMB >= req.MemoryMB &&
		c.Available.GPUs >= req.GPUs &&
		c.Available.DiskMB >= req.DiskMB
}

func (c *ResourceCapacity) Allocate(req ResourceRequirements) error {
	if !c.Fits(req) {
		return fmt.Errorf("insufficient resources: need %s, have %s", req, c.Available)
	}
	c.Available.CPUCores -= req.CPUCores
	c.Available.MemoryMB -= req.MemoryMB
	c.Available.GPUs -= req.GPUs
	c.Available.DiskMB -= req.DiskMB
	return nil
}

func (c *ResourceCapacity) Release(req ResourceRequirements) {
	c.Available.CPUCores += req.CPUCores
	c.Available.MemoryMB += req.MemoryMB
	c.Available.GPUs += req.GPUs
	c.Available.DiskMB += req.DiskMB
	// Clamp to total to prevent overflows from double-release.
	if c.Available.CPUCores > c.Total.CPUCores {
		c.Available.CPUCores = c.Total.CPUCores
	}
	if c.Available.MemoryMB > c.Total.MemoryMB {
		c.Available.MemoryMB = c.Total.MemoryMB
	}
	if c.Available.GPUs > c.Total.GPUs {
		c.Available.GPUs = c.Total.GPUs
	}
	if c.Available.DiskMB > c.Total.DiskMB {
		c.Available.DiskMB = c.Total.DiskMB
	}
}

func (c *ResourceCapacity) UtilizationPercent() float64 {
	if c.Total.CPUCores == 0 && c.Total.MemoryMB == 0 && c.Total.GPUs == 0 && c.Total.DiskMB == 0 {
		return 0
	}
	var totalPercent float64
	var count int

	if c.Total.CPUCores > 0 {
		totalPercent += float64(c.Total.CPUCores-c.Available.CPUCores) / float64(c.Total.CPUCores)
		count++
	}
	if c.Total.MemoryMB > 0 {
		totalPercent += float64(c.Total.MemoryMB-c.Available.MemoryMB) / float64(c.Total.MemoryMB)
		count++
	}
	if c.Total.GPUs > 0 {
		totalPercent += float64(c.Total.GPUs-c.Available.GPUs) / float64(c.Total.GPUs)
		count++
	}
	if c.Total.DiskMB > 0 {
		totalPercent += float64(c.Total.DiskMB-c.Available.DiskMB) / float64(c.Total.DiskMB)
		count++
	}

	if count == 0 {
		return 0
	}
	return (totalPercent / float64(count)) * 100
}

type WorkerState int

const (
	WorkerStateActive   WorkerState = iota
	WorkerStateDraining
	WorkerStateDead
)

func (s WorkerState) String() string {
	switch s {
	case WorkerStateActive:
		return "ACTIVE"
	case WorkerStateDraining:
		return "DRAINING"
	case WorkerStateDead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}
