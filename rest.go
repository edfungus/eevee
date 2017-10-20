package eevee

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

type RestRoute struct {
	Path   string
	Method string
}

type RestConnectionConfig struct {
	Topics map[string]RestRoute
	Port   string
	Router *mux.Router
}

type RestConnection struct {
	Router *mux.Router
	config RestConnectionConfig
	server *http.Server
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
		server: &http.Server{
			Addr:    ":" + config.Port,
			Handler: config.Router,
		},
		in:  make(chan Payload),
		out: make(chan Payload),
	}, nil
}

func (rc *RestConnection) Start(ctx context.Context) {
	for topic, route := range rc.config.Topics {
		rc.Router.Handle(route.Path, rc.requestHandler(topic)).Methods(route.Method)
	}
	go rc.server.ListenAndServe()
	go rc.waitToStopServer(ctx)
}

func (rc *RestConnection) In() <-chan Payload {
	return rc.in
}

func (rc *RestConnection) Out() chan<- Payload {
	return rc.out
}

func (rc *RestConnection) requestHandler(topic string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Debug("REST received message")
		defer w.WriteHeader(http.StatusOK)
		defer r.Body.Close()
		msg := new(bytes.Buffer)
		msg.ReadFrom(r.Body)
		rc.in <- NewPayload(msg.Bytes(), topic)
	})
}

func (rc *RestConnection) waitToStopServer(ctx context.Context) {
	<-ctx.Done()
	timeoutCtx, cancelCtx := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCtx()
	log.Info("Stopping REST Client")
	rc.server.Shutdown(timeoutCtx)
}
