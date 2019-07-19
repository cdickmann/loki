package targets

import (
	"bufio"
	"io"
	"strings"
	"time"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"

	"github.com/grafana/loki/pkg/promtail/api"
	"github.com/grafana/loki/pkg/promtail/positions"
)

type line struct {
	Text string
	Time time.Time
	Err  error
}

type k8sapitailer struct {
	logger    log.Logger
	handler   api.EntryHandler
	positions *positions.Positions

	path string
	// tail *tail.Tail
	ns            string
	podname       string
	containername string

	lineChannel  chan line
	streamReader io.ReadCloser
	lastTime     time.Time

	quit chan struct{}
	done chan struct{}
}

func TailReader(t *k8sapitailer) {
	reader := bufio.NewReader(t.streamReader)

	for {
		s, err := reader.ReadString('\n')
		if len(s) > 0 {
			// K8s prepended a Timestamp and a space, we extract that into a
			// Time object.
			parts := strings.SplitN(s, " ", 2)
			lineTime, err := time.Parse(time.RFC3339, parts[0])

			t.lineChannel <- line{
				Text: parts[1],
				Time: lineTime,
				Err:  err,
			}
		}
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
			}
			break
		}
	}

	close(t.lineChannel)
	fmt.Println("TailChunk NO MORE!")
}

func TailViaApiServer(t *k8sapitailer, namespace string, podname string, containername string, pos string) error {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	fmt.Printf("Logs for ns=[%s] pod=[%s] c=[%s]\n", namespace, podname, containername)
	opts := &corev1.PodLogOptions{
		Container: containername,
		// Container:  thePod.Spec.Containers[0].Name,
		Follow: true,
		//Previous:   false,
		Timestamps: true,
	}
	if pos != "" {
		posTime, err := time.Parse(time.Now().String(), pos)
		if err == nil {
			opts.SinceTime = &metav1.Time{Time: posTime}
		}
	}
	req := clientset.CoreV1().Pods(namespace).GetLogs(podname, opts)

	t.streamReader, err = req.Stream()
	if err != nil {
		return err
	}

	go TailReader(t)

	return nil
}

func newK8sApiTailer(logger log.Logger, handler api.EntryHandler, positions *positions.Positions, path string, ns string, podname string, containername string) (*k8sapitailer, error) {
	pos := positions.GetString(path)

	tailer := &k8sapitailer{
		logger:    logger,
		handler:   api.AddLabelsMiddleware(model.LabelSet{FilenameLabel: model.LabelValue(path)}).Wrap(handler),
		positions: positions,

		path:          path,
		ns:            ns,
		podname:       podname,
		containername: containername,

		lineChannel: make(chan line),
		quit:        make(chan struct{}),
		done:        make(chan struct{}),
	}
	err :	= TailViaApiServer(tailer, ns, podname, containername, pos)
	if err != nil {
		return nil, err
	}

	go tailer.run()
	filesActive.Add(1.)
	return tailer, nil
}

func (t *k8sapitailer) run() {
	level.Info(t.logger).Log("msg", "start tailing via k8s API server", "path", t.path)
	positionSyncPeriod := t.positions.SyncPeriod()
	positionWait := time.NewTicker(positionSyncPeriod)

	defer func() {
		positionWait.Stop()
		close(t.done)
	}()

	for {
		select {
		case <-positionWait.C:
			err := t.markPosition()
			if err != nil {
				level.Error(t.logger).Log("msg", "error getting tail position", "path", t.path, "error", err)
				continue
			}

		case line, ok := <-t.lineChannel:
			if !ok {
				return
			}

			if line.Err != nil {
				level.Error(t.logger).Log("msg", "error reading line", "path", t.path, "error", line.Err)
			}

			readLines.WithLabelValues(t.path).Inc()
			lineLen := float64(len(line.Text))
			readBytes.WithLabelValues(t.path).Add(lineLen)
			logLengthHistogram.WithLabelValues(t.path).Observe(lineLen)
			t.lastTime = line.Time
			if err := t.handler.Handle(model.LabelSet{}, line.Time, line.Text); err != nil {
				level.Error(t.logger).Log("msg", "error handling line", "path", t.path, "error", err)
			}
		case <-t.quit:
			return
		}
	}
}

func (t *k8sapitailer) markPosition() error {
	pos := t.lastTime.String()
	level.Debug(t.logger).Log("path", t.path, "current_position", pos)
	t.positions.PutString(t.path, pos)
	return nil
}

func (t *k8sapitailer) stop() error {
	// Save the current position before shutting down tailer
	err := t.markPosition()
	if err != nil {
		level.Error(t.logger).Log("msg", "error getting tail position", "path", t.path, "error", err)
	}
	// XXX
	t.streamReader.Close()
	// err = t.tail.Stop()
	close(t.quit)
	<-t.done
	filesActive.Add(-1.)
	// When we stop tailing the file, also un-export metrics related to the file
	readBytes.DeleteLabelValues(t.path)
	totalBytes.DeleteLabelValues(t.path)
	logLengthHistogram.DeleteLabelValues(t.path)
	level.Info(t.logger).Log("msg", "stopped tailing file", "path", t.path)
	return err
}

func (t *k8sapitailer) cleanup() {
	t.positions.Remove(t.path)
}
