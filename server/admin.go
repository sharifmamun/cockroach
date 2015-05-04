// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Bram Gruneir (bram.gruneir@gmail.com)

package server

import (
	// This is imported for its side-effect of registering expvar
	// endpoints with the http.DefaultServeMux.
	_ "expvar"
	"fmt"
	"io/ioutil"
	"net/http"
	// This is imported for its side-effect of registering pprof
	// endpoints with the http.DefaultServeMux.
	_ "net/http/pprof"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/util"
	"github.com/julienschmidt/httprouter"
)

const (
	maxGetResults = 0 // TODO(spencer): maybe we need paged query support

	// adminEndpoint is the prefix for RESTful endpoints used to
	// provide an administrative interface to the cockroach cluster.
	adminEndpoint = "/_admin/"
	// debugEndpoint is the prefix of golang's standard debug functionality
	// for access to exported vars and pprof tools.
	debugEndpoint = "/debug/"
	// healthPath is the health endpoint.
	healthPath = adminEndpoint + "health"
	// quitPath is the quit endpoint.
	quitPath = adminEndpoint + "quit"
	// acctPathPrefix is the prefix for accounting configuration changes.
	acctPathPrefix = adminEndpoint + "acct"
	// permPathPrefix is the prefix for permission configuration changes.
	permPathPrefix = adminEndpoint + "perms"
	// zonePathPrefix is the prefix for zone configuration changes.
	zonePathPrefix = adminEndpoint + "zones"
)

// An actionHandler is an interface which provides Get, Put & Delete
// to satisfy administrative REST APIs.
type actionHandler interface {
	Put(path string, body []byte, r *http.Request) error
	Get(path string, r *http.Request) (body []byte, contentType string, err error)
	Delete(path string, r *http.Request) error
}

// A adminServer provides a RESTful HTTP API to administration of
// the cockroach cluster.
type adminServer struct {
	db      *client.KV    // Key-value database client
	stopper *util.Stopper // Used to shutdown the server
	acct    *acctHandler
	perm    *permHandler
	zone    *zoneHandler
}

// newAdminServer allocates and returns a new REST server for
// administrative APIs.
func newAdminServer(db *client.KV, stopper *util.Stopper) *adminServer {
	return &adminServer{
		db:      db,
		stopper: stopper,
		acct:    &acctHandler{db: db},
		perm:    &permHandler{db: db},
		zone:    &zoneHandler{db: db},
	}
}

// registerHandlers registers admin handlers with the supplied
// serve mux.
func (s *adminServer) registerHandlers(router *httprouter.Router) {
	// Pass through requests to /debug to the default serve mux so we
	// get exported variables and pprof tools.
	router.GET("/_admin/acct", s.handleGetAction(s.acct, acctPathPrefix))
	router.GET("/_admin/acct/:key", s.handleGetAction(s.acct, acctPathPrefix))
	router.PUT("/_admin/acct/:key", s.handlePutAction(s.acct, acctPathPrefix))
	router.POST("/_admin/acct/:key", s.handlePutAction(s.acct, acctPathPrefix))
	router.DELETE("/_admin/acct/:key", s.handleDeleteAction(s.acct, acctPathPrefix))

	router.GET("/_admin/perms", s.handleGetAction(s.perm, permPathPrefix))
	router.GET("/_admin/perms/:key", s.handleGetAction(s.perm, permPathPrefix))
	router.PUT("/_admin/perms/:key", s.handlePutAction(s.perm, permPathPrefix))
	router.POST("/_admin/perms/:key", s.handlePutAction(s.perm, permPathPrefix))
	router.DELETE("/_admin/perms/:key", s.handleDeleteAction(s.perm, permPathPrefix))

	router.GET("/_admin/zones", s.handleGetAction(s.zone, zonePathPrefix))
	router.PUT("/_admin/zones/:key", s.handlePutAction(s.zone, zonePathPrefix))
	router.POST("/_admin/zones/:key", s.handlePutAction(s.zone, zonePathPrefix))
	router.DELETE("/_admin/zones/:key", s.handleDeleteAction(s.zone, zonePathPrefix))

	router.GET(healthPath, s.handleHealth)
	router.GET(quitPath, s.handleQuit)

	router.HandlerFunc("GET", debugEndpoint, s.handleDebug)
	router.HandlerFunc("PUT", debugEndpoint, s.handleDebug)
	router.HandlerFunc("POST", debugEndpoint, s.handleDebug)
	router.HandlerFunc("DELETE", debugEndpoint, s.handleDebug)

	a := "/_admin/perms/key0"
	b, c, d := router.Lookup("POST", a)
	fmt.Printf("**** lookup:\n%v\n%v\n%v\n", b, c, d)

}

func (s *adminServer) handleGetAction(handler actionHandler, prefix string) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		path, err := unescapePath(r.URL.Path, prefix)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		b, contentType, err := handler.Get(path, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", contentType)
		fmt.Fprintf(w, "%s", string(b))
	}
}

func (s *adminServer) handlePutAction(handler actionHandler, prefix string) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		fmt.Printf("**** handle put action\n")
		path, err := unescapePath(r.URL.Path, prefix)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()
		if err = handler.Put(path, b, r); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func (s *adminServer) handleDeleteAction(handler actionHandler, prefix string) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		path, err := unescapePath(r.URL.Path, prefix)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err = handler.Delete(path, r); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// handleHealth responds to health requests from monitoring services.
func (s *adminServer) handleHealth(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, "ok")
}

// handleQuit is the shutdown hook. The server is first placed into a
// draining mode, followed by exit.
func (s *adminServer) handleQuit(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, "ok")
	go s.stopper.Stop()
}

// handleDebug passes requests with the debugPathPrefix onto the default
// serve mux, which is preconfigured (by import of expvar and net/http/pprof)
// to serve endpoints which access exported variables and pprof tools.
func (s *adminServer) handleDebug(w http.ResponseWriter, r *http.Request) {
	handler, _ := http.DefaultServeMux.Handler(r)
	handler.ServeHTTP(w, r)
}

func unescapePath(path, prefix string) (string, error) {
	result, err := url.QueryUnescape(strings.TrimPrefix(path, prefix))
	if err != nil {
		return "", err
	}
	return result, nil
}
