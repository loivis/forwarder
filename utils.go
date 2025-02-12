package forwarder

import (
	"context"
	"fmt"
	"strings"

	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
)

var (
	Quiet = true
)

func parseSource(source string) (*Option, error) {
	list := strings.Split(source, "/")
	if len(list) != 2 {
		return nil, fmt.Errorf("invalid source: %v", source)
	}

	kind := list[0]
	name := list[1]

	if kind == "svc" || kind == "service" || kind == "services" {
		return &Option{ServiceName: name}, nil
	}
	if kind == "po" || kind == "pod" || kind == "pods" {
		return &Option{PodName: name}, nil
	}

	return nil, fmt.Errorf("invalid source: %v", source)
}

func parseOptions(options []*Option) ([]*Option, error) {
	newOptions := []*Option{}

	for _, o := range options {
		o := o
		if o.Namespace == "" {
			o.Namespace = "default"
		}
		if o.Source != "" {
			opt, err := parseSource(o.Source)
			if err != nil {
				return nil, err
			}
			if opt.ServiceName != "" {
				o.ServiceName = opt.ServiceName
			}
			if opt.PodName != "" {
				o.PodName = opt.PodName
			}
		}

		if o.Timeout == 0 {
			o.Timeout = defaultTimeout
		}

		if o.PodName == "" && o.ServiceName == "" {
			return nil, fmt.Errorf("please provide a name of pod or service")
		}

		newOptions = append(newOptions, o)
	}

	return newOptions, nil
}

func handleOptions(ctx context.Context, options []*Option, config *restclient.Config) ([]*PodOption, error) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	podOptions := make([]*PodOption, len(options))

	var g errgroup.Group

	for index, option := range options {
		option := option
		index := index

		g.Go(func() error {
			if option.PodName != "" {
				pod, err := clientset.CoreV1().Pods(option.Namespace).Get(ctx, option.PodName, metav1.GetOptions{})
				if err != nil {
					return err
				}
				if pod == nil {
					return fmt.Errorf("no such pod: %v", option.PodName)
				}

				podOptions[index] = buildPodOption(option, pod)
				return nil
			}

			svc, err := clientset.CoreV1().Services(option.Namespace).Get(ctx, option.ServiceName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if svc == nil {
				return fmt.Errorf("no such service: %+v", option.ServiceName)
			}

			labels := []string{}
			for key, val := range svc.Spec.Selector {
				labels = append(labels, key+"="+val)
			}
			label := strings.Join(labels, ",")

			pods, err := clientset.CoreV1().Pods(option.Namespace).List(ctx, metav1.ListOptions{LabelSelector: label})
			if err != nil {
				return err
			}
			if len(pods.Items) == 0 {
				return fmt.Errorf("no such pods of the service of %v", option.ServiceName)
			}

			var pod *v1.Pod

		findPod:
			for _, item := range pods.Items {
				for _, condition := range item.Status.Conditions {
					if condition.Type == v1.PodReady {
						if condition.Status == v1.ConditionTrue {
							pod = &item
							printf("pod(%s) selected in service(%s)\n", item.Name, option.ServiceName)
							break findPod
						}
						printf("pod(%s) in service(%s) is not ready\n", item.Name, option.ServiceName)
					}
				}
			}

			if pod == nil {
				return fmt.Errorf("ready pod not found in service(%s)", option.ServiceName)
			}

			printf("forwarding service(%s) in namespace(%s) to pod(%s) ...\n", option.ServiceName, option.Namespace, pod.Name)

			podOptions[index] = buildPodOption(option, pod)

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return podOptions, nil
}

func buildPodOption(option *Option, pod *v1.Pod) *PodOption {
	if option.RemotePort == 0 {
		option.RemotePort = int(pod.Spec.Containers[0].Ports[0].ContainerPort)
	}

	return &PodOption{
		LocalPort: option.LocalPort,
		PodPort:   option.RemotePort,
		Pod: v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pod.Name,
				Namespace: pod.Namespace,
			},
		},
	}
}

func printf(format string, v ...any) {
	if !Quiet {
		fmt.Printf(format+"\n", v...)
	}
}
