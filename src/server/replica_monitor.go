package server

import (
	"context"
	"sync"
	"time"

	"github.com/data-dispatcher-service/src/config"
	"github.com/sirupsen/logrus"
)



type ReplicaMonitor struct {
	logger              *logrus.Logger
	config              config.Interface
	orchestrator        Orchestrator
	stateMutex          sync.Mutex
	activeWorkers       int
	minThreshold        int
	minThresholdMet     bool
	maxThreshold        int
	maxThresholdReached bool
	isLeader            bool
	wg                  sync.WaitGroup
	startupTimer        *time.Timer
	ctx                 context.Context
	cancel              context.CancelFunc
}

// NewReplicaMonitor creates a new instance of ReplicaMonitor.
func NewReplicaMonitor(cfg config.Interface, logger *logrus.Logger, orchestrator Orchestrator) *ReplicaMonitor {
	ctx, cancel := context.WithCancel(context.Background())

	return &ReplicaMonitor{
		logger:              logger,
		config:              cfg,
		orchestrator:        orchestrator,
		activeWorkers:       0,
		minThreshold:        cfg.GetMinThreshold(),
		minThresholdMet:     false,
		maxThreshold:        cfg.GetWorkerPoolSize(),
		maxThresholdReached: false,
		isLeader:            cfg.IsLeader(),
		ctx:                 ctx,
		cancel:              cancel,
		wg:                  sync.WaitGroup{},
	}
}

// Start begins the monitoring process of ReplicaMonitor.
func (m *ReplicaMonitor) Start() {
	if m.isLeader {
		m.logger.Info("Replica Monitor: Leader detected. Monitoring not required.")
		return
	}

	timeoutDuration := m.config.GetStartupTimeout()
	m.logger.WithFields(logrus.Fields{
		"threshold": m.minThreshold,
		"timeout":   timeoutDuration,
	}).Info("Replica Monitor: Non-leader started. Activating startup timer.")

	m.startupTimer = time.NewTimer(timeoutDuration)

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		select {
		case <-m.startupTimer.C:
			// Timer fired
			m.stateMutex.Lock()
			if !m.minThresholdMet {
				// Threshold not met in time
				m.logger.WithField("timeout", timeoutDuration).Warn("Replica Monitor: Startup timer EXPIRED. Requesting shutdown.")
				// Request shutdown
				go m.orchestrator.RequestShutdown()
			} else {
				m.logger.Debug("Replica Monitor: Timer fired, but threshold already met.")
			}
			m.stateMutex.Unlock()

		case <-m.ctx.Done():
			// Graceful shutdown requested
			m.logger.Debug("Replica Monitor: Shutdown requested. Stopping startup timer.")
			if !m.startupTimer.Stop() {
				<-m.startupTimer.C // just for cleanup
			}
			return
		}
	}()
}

// NotifyWorkerStart is called when a worker starts a task.
func (m *ReplicaMonitor) NotifyWorkerStart() {
	m.stateMutex.Lock()
	defer m.stateMutex.Unlock()

	m.activeWorkers++
	m.logger.WithField("active_workers", m.activeWorkers).Debug("Replica Monitor: Worker started a task")

	if !m.isLeader && !m.minThresholdMet && m.activeWorkers >= m.minThreshold {
		m.logger.WithFields(logrus.Fields{
			"active_workers": m.activeWorkers,
			"threshold":      m.minThreshold,
		}).Info("Replica Monitor: Minimum threshold REACHED. Stopping startup timer.")

		m.minThresholdMet = true
		if m.startupTimer != nil {
			if !m.startupTimer.Stop() {
				select {
				case <-m.startupTimer.C:
				default:
				}
			}
		}
	}

	if !m.isLeader && !m.maxThresholdReached && m.activeWorkers == m.maxThreshold {
		m.logger.WithFields(logrus.Fields{
			"active_workers": m.activeWorkers,
			"threshold":      m.maxThreshold,
		}).Warn("Replica Monitor: MAX threshold REACHED. Requesting scale-up.")

		// set flag to avoid multiple scale-up requests
		m.maxThresholdReached = true

		// Request scale-up
		go m.orchestrator.RequestScaleUp()
	}
}

// NotifyWorkerFinish is called when a worker finishes a task.
func (m *ReplicaMonitor) NotifyWorkerFinish() {
	m.stateMutex.Lock()
	defer m.stateMutex.Unlock()

	m.activeWorkers--
	m.logger.WithField("active_workers", m.activeWorkers).Debug("Replica Monitor: Worker finished a task")

	if !m.isLeader && !m.minThresholdMet && m.activeWorkers < m.minThreshold {
		m.logger.WithFields(logrus.Fields{
			"active_workers": m.activeWorkers,
			"threshold":      m.minThreshold,
		}).Warn("Replica Monitor: Active workers FELL BELOW threshold. Requesting shutdown.")

		// Request shutdown
		go m.orchestrator.RequestShutdown()
	}

	// if we had requested scale-up before, but load has dropped
	// we reset the flag to allow future scale-up requests
	// but we use a 90% threshold to avoid rapid toggling
	// e.g if max was 10, we reset when load drops to 7 capacity
	resetThreshold := int(float64(m.maxThreshold) * config.RESET_CAPACITY)
	if m.maxThresholdReached && m.activeWorkers <= resetThreshold {
		m.logger.WithFields(logrus.Fields{
			"active_workers": m.activeWorkers,
			"reset_at":       resetThreshold,
		}).Info("Replica Monitor: Load dropped below 90%. Re-enabling max threshold trigger.")
		m.maxThresholdReached = false
	}
}

func (m *ReplicaMonitor) Stop() {
	m.logger.Info("Replica Monitor: Stopping monitor.")
	m.cancel()  // Signals the timer goroutine
	m.wg.Wait() // Waits for the timer goroutine to finish
}
