package datadog_executor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
)

// MonitorPoller periodically queries Datadog for monitor statuses and emits
// DatadogMonitorAlert signals when a monitor transitions state.
type MonitorPoller struct {
	executor *DatadogExecutor
	alertCh  chan<- *v1.DatadogMonitorAlert
	logger   *zap.Logger
	interval time.Duration

	mu             sync.Mutex
	lastStatuses   map[string]string // monitorID -> last known overall_state
	initialPollDone bool

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewMonitorPoller creates a new poller that emits alerts on the provided channel.
func NewMonitorPoller(executor *DatadogExecutor, alertCh chan<- *v1.DatadogMonitorAlert, logger *zap.Logger) *MonitorPoller {
	interval := 60 * time.Second
	if envInterval := getEnvInt("DD_MONITOR_POLL_INTERVAL_SECONDS"); envInterval > 0 {
		interval = time.Duration(envInterval) * time.Second
	}
	return &MonitorPoller{
		executor:     executor,
		alertCh:      alertCh,
		logger:       logger.Named("dd-monitor-poller"),
		interval:     interval,
		lastStatuses: make(map[string]string),
	}
}

// Start begins the polling loop. It blocks until ctx is cancelled or Stop is called.
func (p *MonitorPoller) Start(ctx context.Context) {
	ctx, p.cancel = context.WithCancel(ctx)
	p.wg.Add(1)
	go p.run(ctx)
}

// Stop gracefully stops the polling loop.
func (p *MonitorPoller) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
}

func (p *MonitorPoller) run(ctx context.Context) {
	defer p.wg.Done()

	p.logger.Info("Starting Datadog monitor poller", zap.Duration("interval", p.interval))

	// Initial poll — store states without emitting
	if err := p.poll(ctx, true); err != nil {
		p.logger.Warn("Initial Datadog monitor poll failed", zap.Error(err))
	}

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Datadog monitor poller stopped")
			return
		case <-ticker.C:
			if err := p.poll(ctx, false); err != nil {
				p.logger.Warn("Datadog monitor poll failed", zap.Error(err))
			}
		}
	}
}

type datadogMonitorResponse struct {
	ID           int64    `json:"id"`
	Name         string   `json:"name"`
	Type         string   `json:"type"`
	Query        string   `json:"query"`
	Message      string   `json:"message"`
	Tags         []string `json:"tags"`
	OverallState string   `json:"overall_state"` // "Alert", "Warn", "OK", "No Data", "Ignored", "Skipped"
}

func (p *MonitorPoller) poll(ctx context.Context, silent bool) error {
	if err := p.executor.ensureDiscovered(ctx); err != nil {
		return fmt.Errorf("datadog not discovered: %w", err)
	}

	monitors, err := p.fetchMonitors(ctx)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now().Unix()

	for _, mon := range monitors {
		monID := strconv.FormatInt(mon.ID, 10)
		prevStatus, exists := p.lastStatuses[monID]
		currentStatus := normalizeStatus(mon.OverallState)

		p.lastStatuses[monID] = currentStatus

		if !exists || silent {
			continue
		}

		if prevStatus != currentStatus && currentStatus != "" {
			alert := &v1.DatadogMonitorAlert{
				MonitorId:      monID,
				MonitorName:    mon.Name,
				CurrentStatus:  currentStatus,
				PreviousStatus: prevStatus,
				MonitorType:    mon.Type,
				Query:          mon.Query,
				Message:        mon.Message,
				Tags:           mon.Tags,
				Timestamp:      now,
			}

			p.logger.Info("Datadog monitor state transition",
				zap.String("monitor_id", monID),
				zap.String("name", mon.Name),
				zap.String("from", prevStatus),
				zap.String("to", currentStatus))

			select {
			case p.alertCh <- alert:
			case <-ctx.Done():
				return ctx.Err()
			default:
				p.logger.Warn("Alert channel full, dropping Datadog monitor alert",
					zap.String("monitor_id", monID))
			}
		}
	}

	if !p.initialPollDone {
		p.initialPollDone = true
		p.logger.Info("Initial Datadog monitor poll complete", zap.Int("monitors", len(monitors)))
	}

	return nil
}

func (p *MonitorPoller) fetchMonitors(ctx context.Context) ([]datadogMonitorResponse, error) {
	// Use the executor's doGet to query monitors
	body, statusCode, err := p.executor.doGet(ctx, "/api/v1/monitor", nil, true)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch monitors: %w", err)
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("datadog API returned %d: %s", statusCode, truncate(string(body), 200))
	}

	var monitors []datadogMonitorResponse
	if err := json.Unmarshal(body, &monitors); err != nil {
		return nil, fmt.Errorf("failed to parse monitors response: %w", err)
	}

	return monitors, nil
}

func normalizeStatus(raw string) string {
	switch strings.ToLower(raw) {
	case "alert":
		return "Alert"
	case "warn":
		return "Warn"
	case "ok":
		return "OK"
	case "no data":
		return "No Data"
	case "ignored", "skipped":
		return ""
	default:
		return raw
	}
}

func getEnvInt(key string) int {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return 0
	}
	n, _ := strconv.Atoi(val)
	return n
}
