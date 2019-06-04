package client

func applyOptions(opts ...ClientOption) *clientOptions {
	options := &clientOptions{}
	for _, opt := range opts {
		opt.apply(options)
	}
	return options
}

type clientOptions struct {
	application string
	namespace   string
}

type ClientOption interface {
	apply(options *clientOptions)
}

type applicationOption struct {
	application string
}

func (o *applicationOption) apply(options *clientOptions) {
	options.application = o.application
}

func WithApplication(application string) ClientOption {
	return &applicationOption{application: application}
}

type namespaceOption struct {
	namespace string
}

func (o *namespaceOption) apply(options *clientOptions) {
	options.namespace = o.namespace
}

func WithNamespace(namespace string) ClientOption {
	return &namespaceOption{namespace: namespace}
}
