package targets

import (
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"

	"github.com/grafana/loki/pkg/helpers"
	"github.com/grafana/loki/pkg/promtail/api"
	"github.com/grafana/loki/pkg/promtail/positions"
)

// FileTarget describes a particular set of logs.
type K8sApiTailerTarget struct {
	logger log.Logger

	handler          api.EntryHandler
	positions        *positions.Positions
	labels           model.LabelSet
	discoveredLabels model.LabelSet

	watches map[string]struct{}
	path    string
	quit    chan struct{}
	done    chan struct{}

	tail *k8sapitailer

	targetConfig *Config
}

// NewK8sApiTailerTarget create a new K8sApiTailerTarget.
func NewK8sApiTailerTarget(logger log.Logger, handler api.EntryHandler, positions *positions.Positions, path string, labels model.LabelSet, discoveredLabels model.LabelSet, targetConfig *Config) (FilelikeTarget, error) {

	t := &K8sApiTailerTarget{
		logger:           logger,
		path:             path,
		labels:           labels,
		discoveredLabels: discoveredLabels,
		handler:          api.AddLabelsMiddleware(labels).Wrap(handler),
		positions:        positions,
		quit:             make(chan struct{}),
		done:             make(chan struct{}),
		tail:             nil,
		targetConfig:     targetConfig,
	}

	err := t.sync()
	if err != nil {
		return nil, errors.Wrap(err, "k8sapitailertarget.sync")
	}

	go t.run()
	return t, nil
}

// Ready if at least one file is being tailed
func (t *K8sApiTailerTarget) Ready() bool {
	return t.tail != nil
}

// Stop the target.
func (t *K8sApiTailerTarget) Stop() {
	close(t.quit)
	<-t.done
}

// Type implements a Target
func (t *K8sApiTailerTarget) Type() TargetType {
	return K8sApiTailerTargetType
}

// DiscoveredLabels implements a Target
func (t *K8sApiTailerTarget) DiscoveredLabels() model.LabelSet {
	return t.discoveredLabels
}

// Labels implements a Target
func (t *K8sApiTailerTarget) Labels() model.LabelSet {
	return t.labels
}

// Details implements a Target
func (t *K8sApiTailerTarget) Details() interface{} {
	files := map[string]int64{}
	files[t.path], _ = t.positions.Get(t.path)
	return files
}

func (t *K8sApiTailerTarget) run() {
	defer func() {
		helpers.LogError("updating tailer last position", t.tail.markPosition)
		helpers.LogError("stopping tailer", t.tail.stop)
		level.Debug(t.logger).Log("msg", "watcher closed, tailer stopped, positions saved")
		close(t.done)
	}()

	ticker := time.NewTicker(t.targetConfig.SyncPeriod)

	for {
		select {
		case <-ticker.C:
			err := t.sync()
			if err != nil {
				level.Error(t.logger).Log("msg", "error running sync function", "error", err)
			}
		case <-t.quit:
			return
		}
	}
}

// Start the tail if not started already
func (t *K8sApiTailerTarget) sync() error {
	// XXX: Should check if tail connection is broken and restart if so
	if t.tail != nil {
		return nil
	}
	level.Info(t.logger).Log("msg", "newFileTarget",
		"positions", t.positions,
		"path", t.path,
		"namespace", t.labels["namespace"],
		"pod", t.labels["instance"],
		"container_name", t.labels["container_name"])
	tailer, err := newK8sApiTailer(t.logger, t.handler, t.positions, t.path,
		string(t.labels["namespace"]), string(t.labels["instance"]),
		string(t.labels["container_name"]))
	if err != nil {
		level.Error(t.logger).Log("msg", "failed to start tailer", "error", err, "path", t.path)
		return err
	}
	t.tail = tailer

	return nil
}
