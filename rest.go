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
	Topics  map[string]RestRoute
	Port    string
	Router  *mux.Router
	Wrapper func(http.Handler) http.Handler
}

type RestConnection struct {
	router      *mux.Router
	config      RestConnectionConfig
	server      *http.Server
	in          chan Payload
	out         chan Payload
	routeStatus chan RouteStatus
}

func NewRestConnection(config RestConnectionConfig) (*RestConnection, error) {
	var router *mux.Router
	if config.Router != nil {
		router = config.Router
	} else {
		router = mux.NewRouter()
	}
	handler := createHandler(config.Wrapper, router)

	return &RestConnection{
		router: router,
		config: config,
		server: &http.Server{
			Addr:    ":" + config.Port,
			Handler: handler,
		},
		in:          make(chan Payload),
		out:         make(chan Payload),
		routeStatus: make(chan RouteStatus),
	}, nil
}

func (rc *RestConnection) Start(ctx context.Context) {
	for topic, route := range rc.config.Topics {
		rc.router.Handle(route.Path, rc.requestHandler(topic)).Methods(route.Method)
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

func (rc *RestConnection) RouteStatus() chan<- RouteStatus {
	return rc.routeStatus
}

func (rc *RestConnection) requestHandler(topic string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Debug("REST received message")
		defer r.Body.Close()
		msg := new(bytes.Buffer)
		msg.ReadFrom(r.Body)
		rc.in <- NewPayload(msg.Bytes(), topic)
		status := <-rc.routeStatus
		if status.Code == RouteOK {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(status.Message))
		}
	})
}

func (rc *RestConnection) waitToStopServer(ctx context.Context) {
	<-ctx.Done()
	timeoutCtx, cancelCtx := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCtx()
	log.Info("Stopping REST Client")
	rc.server.Shutdown(timeoutCtx)
}

func createHandler(wrapper func(http.Handler) http.Handler, router *mux.Router) (handler http.Handler) {
	if wrapper != nil {
		handler = wrapper(router)
	} else {
		handler = router
	}
	return handler
}
