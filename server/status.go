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
// Author: Shawn Morel (shawn@strangemonad.com)

package server

import (
	"net/http"
	"runtime"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
	"github.com/julienschmidt/httprouter"
)

const (
	// stackTraceApproxSize is the approximate size of a goroutine stack trace.
	stackTraceApproxSize = 1024

	// statusKeyPrefix is the root of the RESTful cluster statistics and metrics API.
	statusKeyPrefix = "/_status/"

	// statusGossipKeyPrefix exposes a view of the gossip network.
	statusGossipKeyPrefix = statusKeyPrefix + "gossip"

	// statusLocalKeyPrefix is the key prefix for all local status
	// info. Unadorned, the URL exposes the status of the node serving
	// the request.  This is equivalent to GETing
	// statusNodesKeyPrefix/<current-node-id>.  Useful for debugging
	// nodes that aren't communicating with the cluster properly.
	statusLocalKeyPrefix = statusKeyPrefix + "local/"

	// statusLocalStacksKey exposes stack traces of running goroutines.
	statusLocalStacksKey = statusLocalKeyPrefix + "stacks"

	// statusNodeKeyPrefix exposes status for each of the nodes the cluster.
	// statusNodeKeyPrefix/nodes -> lists all nodes
	// statusNodeKeyPrefix/nodes/ -> lists all nodes
	// statusNodeKeyPrefix/nodes/{NodeID} -> shows only the status for that
	//                                        specific node
	statusNodesKeyPrefix = statusKeyPrefix + "nodes/"
	statusNodeKeyPrefix  = statusKeyPrefix + "nodes/:id"

	// statusStoreKeyPrefix exposes status for each store.
	// statusStoreKeyPrefix/stores -> lists all nodes
	// statusStoreKeyPrefix/stores/ -> lists all nodes
	// statusStoreKeyPrefix/stores/{StoreID} -> shows only the status for that
	//                                        specific store
	statusStoresKeyPrefix = statusKeyPrefix + "stores/"
	statusStoreKeyPrefix  = statusKeyPrefix + "stores/:id"

	// statusTransactionsKeyPrefix exposes transaction statistics.
	statusTransactionsKeyPrefix = statusKeyPrefix + "txns/"
)

// A statusServer provides a RESTful status API.
type statusServer struct {
	db     *client.KV
	gossip *gossip.Gossip
}

// newStatusServer allocates and returns a statusServer.
func newStatusServer(db *client.KV, gossip *gossip.Gossip) *statusServer {
	return &statusServer{
		db:     db,
		gossip: gossip,
	}
}

// registerHandlers registers admin handlers with the supplied
// serve mux.
func (s *statusServer) registerHandlers(router *httprouter.Router) {
	router.GET(statusKeyPrefix, s.handleClusterStatus)
	router.GET(statusGossipKeyPrefix, s.handleGossipStatus)
	router.GET(statusLocalKeyPrefix, s.handleLocalStatus)
	router.GET(statusLocalStacksKey, s.handleLocalStacks)
	router.GET(statusNodeKeyPrefix, s.handleNodeStatus)
	router.GET(statusNodesKeyPrefix, s.handleNodesStatus)
	router.GET(statusStoresKeyPrefix, s.handleStoresStatus)
	router.GET(statusStoreKeyPrefix, s.handleStoreStatus)
	router.GET(statusTransactionsKeyPrefix, s.handleTransactionStatus)
}

// handleStatus handles GET requests for cluster status.
func (s *statusServer) handleClusterStatus(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	cluster := &status.Cluster{}
	b, contentType, err := util.MarshalResponse(r, cluster, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(b)
}

// handleGossipStatus handles GET requests for gossip network status.
func (s *statusServer) handleGossipStatus(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	b, err := s.gossip.GetInfosAsJSON()
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.Write(b)
}

// handleLocalStatus handles GET requests for local-node status.
func (s *statusServer) handleLocalStatus(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	local := struct {
		BuildInfo util.BuildInfo `json:"buildInfo"`
	}{
		BuildInfo: util.GetBuildInfo(),
	}
	b, contentType, err := util.MarshalResponse(r, local, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(b)
}

// handleLocalStacks handles GET requests for goroutines stack traces.
func (s *statusServer) handleLocalStacks(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	bufSize := runtime.NumGoroutine() * stackTraceApproxSize
	for {
		buf := make([]byte, bufSize)
		length := runtime.Stack(buf, true)
		// If this wasn't large enough to accommodate the full set of
		// stack traces, increase by 2 and try again.
		if length == bufSize {
			bufSize = bufSize * 2
			continue
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Write(buf[:length])
		return
	}
}

// handleNodesStatus handles GET requests for all node statuses.
func (s *statusServer) handleNodesStatus(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	startKey := engine.KeyStatusNodePrefix
	endKey := startKey.PrefixEnd()

	call := client.Scan(startKey, endKey, 0)
	resp := call.Reply.(*proto.ScanResponse)
	if err := s.db.Run(call); err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if resp.Error != nil {
		log.Error(resp.Error)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	nodeStatuses := []proto.NodeStatus{}
	for _, row := range resp.Rows {
		nodeStatus := &proto.NodeStatus{}
		if err := gogoproto.Unmarshal(row.Value.GetBytes(), nodeStatus); err != nil {
			log.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		nodeStatuses = append(nodeStatuses, *nodeStatus)
	}
	b, contentType, err := util.MarshalResponse(r, nodeStatuses, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(b)
}

// handleNodeStatus handles GET requests for a single node's status. If no id is
// available, it calls handleNodesStatus to return all node's statuses.
func (s *statusServer) handleNodeStatus(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{}`))
}

// handleStoresStatus handles GET requests for all store statuses.
func (s *statusServer) handleStoresStatus(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	startKey := engine.KeyStatusStorePrefix
	endKey := startKey.PrefixEnd()

	call := client.Scan(startKey, endKey, 0)
	resp := call.Reply.(*proto.ScanResponse)
	if err := s.db.Run(call); err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if resp.Error != nil {
		log.Error(resp.Error)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	storeStatuses := []proto.StoreStatus{}
	for _, row := range resp.Rows {
		storeStatus := &proto.StoreStatus{}
		if err := gogoproto.Unmarshal(row.Value.GetBytes(), storeStatus); err != nil {
			log.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		storeStatuses = append(storeStatuses, *storeStatus)
	}
	b, contentType, err := util.MarshalResponse(r, storeStatuses, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(b)
}

// handleStoreStatus handles GET requests for a single node's status. If no id
// is available, it calls handleStoresStatus to return all store's statuses.
func (s *statusServer) handleStoreStatus(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{}`))
}

// handleTransactionStatus handles GET requests for transaction status.
func (s *statusServer) handleTransactionStatus(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"transactions": []}`))
}
