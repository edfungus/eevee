package eevee

import (
	"bytes"
	"net/http"

	"github.com/gorilla/mux"
)

type RestRoutes struct {
	Topic  string
	Path   string
	Method string
}

type RestConnectionConfig struct {
	Topics []RestRoutes
	Port   int
	Router *mux.Router
}

type RestConnection struct {
	Router *mux.Router
	config RestConnectionConfig
	in     chan Payload
	out    chan Payload
}

func NewRestConnection(config RestConnectionConfig) (*RestConnection, error) {
	if config.Router == nil {
		config.Router = mux.NewRouter()
	}
	return &RestConnection{
		Router: config.Router,
		config: config,
		in:     make(chan Payload),
		out:    make(chan Payload),
	}, nil
}

func (rc *RestConnection) Start() {
	for _, topic := range rc.config.Topics {
		rc.Router.Handle("/", rc.requestHandler(topic.Topic)).Methods(topic.Method)
	}
}

func (rc *RestConnection) In() <-chan Payload {
	return rc.in
}

func (rc *RestConnection) Out() chan<- Payload {
	return rc.out
}

func (rc *RestConnection) requestHandler(topic string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer w.WriteHeader(http.StatusOK)
		defer r.Body.Close()
		msg := new(bytes.Buffer)
		msg.ReadFrom(r.Body)
		rc.in <- NewPayload(msg.Bytes(), topic)
	})
}
