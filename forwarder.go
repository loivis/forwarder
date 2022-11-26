package forwarder

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"
	ccmd "k8s.io/client-go/tools/clientcmd"
	ccmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

var (
	defaultTimeout = 1 * time.Second
	once           sync.Once
)

// FromConfigBytes returns Forwarders from kube config bytes.
func FromConfigBytes(ctx context.Context, options []*Option, kubeconfigBytes []byte) (*Forwarders, error) {
	kubeconfigGetter := func() (*ccmdapi.Config, error) {
		config, err := shimLoadConfig(kubeconfigBytes)
		if err != nil {
			return nil, err
		}

		return config, nil
	}

	config, err := ccmd.BuildConfigFromKubeconfigGetter("", kubeconfigGetter)
	if err != nil {
		return nil, err
	}

	return forwarders(ctx, options, config)
}

// FromConfigPath returns Forwarders from kube config path.
func FromConfigPath(ctx context.Context, options []*Option, kubeconfigPath string) (*Forwarders, error) {
	if kubeconfigPath == "" {
		kubeconfigPath = filepath.Join(os.Getenv("HOME"), ".kube/config")
	}

	config, err := ccmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, err
	}

	return forwarders(ctx, options, config)
}

// fromRestConfig returns Forwarders from k8s.io/client-go/rest.Config.
func fromRestConfig(ctx context.Context, options []*Option, config *rest.Config) (*Forwarders, error) {
	return forwarders(ctx, options, config)
}

// forwarders returns Forwarders for kubernetes.
func forwarders(ctx context.Context, options []*Option, config *rest.Config) (*Forwarders, error) {
	newOptions, err := parseOptions(options)
	if err != nil {
		return nil, err
	}

	podOptions, err := handleOptions(ctx, newOptions, config)
	if err != nil {
		return nil, err
	}

	stream := genericclioptions.IOStreams{
		In:     os.Stdin,
		Out:    io.Discard,
		ErrOut: os.Stderr,
	}

	carriers := make([]*carrier, len(podOptions))

	var g errgroup.Group

	for index, option := range podOptions {
		index := index
		stopCh := make(chan struct{}, 1)
		readyCh := make(chan struct{})

		req := &request{
			RestConfig: config,
			Pod:        option.Pod,
			LocalPort:  option.LocalPort,
			PodPort:    option.PodPort,
			Streams:    stream,
			StopCh:     stopCh,
			ReadyCh:    readyCh,
		}
		g.Go(func() error {
			errorCh := make(chan error, 1)
			pf, err := forwardPort(req, errorCh)
			if err != nil {
				return err
			}
			carriers[index] = &carrier{ErrorCh: errorCh, StopCh: stopCh, ReadyCh: readyCh, Timeout: newOptions[index].Timeout, PF: pf}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	result := &Forwarders{
		Close: func() {
			once.Do(func() {
				for _, c := range carriers {
					close(c.StopCh)
				}
			})
		},
		Ready: func() ([][]portforward.ForwardedPort, error) {
			pfs := [][]portforward.ForwardedPort{}
			for _, c := range carriers {
				select {
				case <-time.After(c.Timeout):
					return nil, fmt.Errorf("timeout after %v", c.Timeout)
				case err := <-c.ErrorCh:
					return nil, err
				case <-c.ReadyCh:
					ports, err := c.PF.GetPorts()
					if err != nil {
						return nil, err
					}
					pfs = append(pfs, ports)
				}
			}
			return pfs, nil
		},
	}

	result.Wait = func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs
		fmt.Println("Bye...")
		result.Close()
	}

	go func() {
		<-ctx.Done()
		result.Close()
	}()

	return result, nil
}

// forwardPort forwards ports, and returns a forwarder.
func forwardPort(req *request, errorCh chan error) (*portforward.PortForwarder, error) {
	targetURL, err := url.Parse(req.RestConfig.Host)
	if err != nil {
		return nil, err
	}

	targetURL.Path = path.Join(
		targetURL.Path,
		"api", "v1",
		"namespaces", req.Pod.Namespace,
		"pods", req.Pod.Name,
		"portforward",
	)

	transport, upgrader, err := spdy.RoundTripperFor(req.RestConfig)
	if err != nil {
		return nil, err
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, targetURL)
	fw, err := portforward.New(dialer, []string{fmt.Sprintf("%d:%d", req.LocalPort, req.PodPort)}, req.StopCh, req.ReadyCh, req.Streams.Out, req.Streams.ErrOut)
	if err != nil {
		return nil, err
	}

	go func() {
		if err := fw.ForwardPorts(); err != nil {
			errorCh <- err
		}
	}()

	return fw, nil
}

// shimLoadConfig transforms kubeconfig bytes to clientcmdapi config.
func shimLoadConfig(kubeconfigBytes []byte) (*ccmdapi.Config, error) {
	config, err := ccmd.Load(kubeconfigBytes)
	if err != nil {
		return nil, err
	}

	// set LocationOfOrigin on every Cluster, User, and Context
	for key, obj := range config.AuthInfos {
		config.AuthInfos[key] = obj
	}
	for key, obj := range config.Clusters {
		config.Clusters[key] = obj
	}
	for key, obj := range config.Contexts {
		config.Contexts[key] = obj
	}

	if config.AuthInfos == nil {
		config.AuthInfos = map[string]*ccmdapi.AuthInfo{}
	}
	if config.Clusters == nil {
		config.Clusters = map[string]*ccmdapi.Cluster{}
	}
	if config.Contexts == nil {
		config.Contexts = map[string]*ccmdapi.Context{}
	}

	return config, nil
}
