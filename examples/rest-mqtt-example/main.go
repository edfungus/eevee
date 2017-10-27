package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/edfungus/eevee"
	logging "github.com/op/go-logging"
)

/*
A uni-directional example routing rest calls to mqtt
*/

var log = logging.MustGetLogger("eevee")

func main() {
	eevee.StartLogger()
	log.Notice("Eevee is starting up")

	restConfig := eevee.RestConnectionConfig{
		Topics: map[string]eevee.RestRoute{
			"open": {
				Path:   "/open",
				Method: http.MethodGet, // Just showing that different methods also work
			},
			"close": {
				Path:   "/close",
				Method: http.MethodPost,
			},
		},
		Port:    "8080",
		Router:  nil,
		Wrapper: basicAuthWrapper,
	}
	restConnection, err := eevee.NewRestConnection(restConfig)
	if err != nil {
		log.Fatalf("Could not create Rest Connection with err: %s", err.Error())
	}

	mqttConfig := eevee.MqttConnectionConfig{
		Server:   "tcp://localhost:1883",
		Topics:   []string{"open", "close"},
		ClientID: "eevee",
		Qos:      byte(1),
	}
	mqttConnetion, err := eevee.NewMqttConnection(mqttConfig)
	if err != nil {
		log.Fatalf("Could not create MQTT Client with err: %s", err.Error())
	}

	jsonTranslator := NewJsonTranslator()
	dumbTranslator := NewDumbTranslator()
	mapStore := NewMapStore()
	dumbStore := NewDumbStore()

	restConnector := &eevee.Connector{
		Connection: restConnection,
		Translator: dumbTranslator,
		IDStore:    dumbStore,
	}

	mqttConnector := &eevee.Connector{
		Connection: mqttConnetion,
		Translator: jsonTranslator,
		IDStore:    mapStore,
	}

	bridge := eevee.NewUniBridge(restConnector, mqttConnector)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	bridge.Start(ctx)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	<-sigchan
	cancel()
	time.Sleep(time.Second * 1)
	log.Noticef("Eevee says goodbye")
}

func basicAuthWrapper(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if u == "user" && p == "password" {
			h.ServeHTTP(w, r)
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
		return
	})
}
