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

package server

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/julienschmidt/httprouter"
)

// startStatusServer launches a new status server using minimal engine
// and local database setup. Returns the new http test server, which
// should be cleaned up by caller via httptest.Server.Close(). The
// Cockroach KV client address is set to the address of the test server.
func startStatusServer() (*httptest.Server, *util.Stopper) {
	stopper := util.NewStopper()
	db, err := BootstrapCluster("cluster-1", []engine.Engine{engine.NewInMem(proto.Attributes{}, 1<<20)}, stopper)
	if err != nil {
		log.Fatal(err)
	}
	status := newStatusServer(db, nil)
	router := httprouter.New()
	status.registerHandlers(router)
	httpServer := httptest.NewTLSServer(router)
	stopper.AddCloser(httpServer)
	return httpServer, stopper
}

// TestStatusLocalStacks verifies that goroutine stack traces are available
// via the /_status/local/stacks endpoint.
func TestStatusLocalStacks(t *testing.T) {
	s, stopper := startStatusServer()
	defer stopper.Stop()
	body, err := getText(s.URL + statusLocalStacksKey)
	if err != nil {
		t.Fatal(err)
	}
	// Verify match with at least two goroutine stacks.
	if matches, err := regexp.Match("(?s)goroutine [0-9]+.*goroutine [0-9]+.*", body); !matches || err != nil {
		t.Errorf("expected match: %t; err nil: %s", matches, err)
	}
}

// TestStatusJson verifies that status endpoints return expected
// Json results. The content type of the responses is always
// "application/json".
func TestStatusJson(t *testing.T) {
	s := StartTestServer(t)
	defer s.Stop()

	type TestCase struct {
		keyPrefix string
		expected  string
	}

	testCases := []TestCase{
		{statusKeyPrefix, "{}"},
	}
	// Test the /_status/local/ endpoint only in a go release branch.
	if !strings.HasPrefix(runtime.Version(), "devel") {
		testCases = append(testCases, TestCase{statusLocalKeyPrefix, `{
  "buildInfo": {
    "goVersion": "go[0-9\.]+",
    "tag": "",
    "time": "",
    "dependencies": ""
  }
}`})
	}

	httpClient, err := testContext.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range testCases {
		contentTypes := []string{"application/json", "application/x-protobuf", "text/yaml"}
		for _, contentType := range contentTypes {
			req, err := http.NewRequest("GET", testContext.RequestScheme()+"://"+s.ServingAddr()+spec.keyPrefix, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Accept", contentType)
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Errorf("unexpected status code: %v", resp.StatusCode)
			}
			returnedContentType := resp.Header.Get(util.ContentTypeHeader)
			if returnedContentType != "application/json" {
				t.Errorf("unexpected content type: %v", returnedContentType)
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if matches, err := regexp.Match(spec.expected, body); !matches || err != nil {
				t.Errorf("expected match: %t; err nil: %s", matches, err)
			}
		}
	}
}

// TestStatusGossipJson ensures that the output response for the full gossip
// info contains the required fields.
func TestStatusGossipJson(t *testing.T) {
	s := StartTestServer(t)
	defer s.Stop()

	type prefixedInfo struct {
		Key string                 `json:"Key"`
		Val []storage.PrefixConfig `json:"Val"`
	}

	type rangeDescriptorInfo struct {
		Key string                `json:"Key"`
		Val proto.RangeDescriptor `json:"Val"`
	}

	type keyValueStringPair struct {
		Key string `json:"Key"`
		Val string `json:"Val"`
	}

	type keyValuePair struct {
		Key string                 `json:"Key"`
		Val map[string]interface{} `json:"Val"`
	}

	type infos struct {
		Infos struct {
			Accounting  *prefixedInfo        `json:"accounting"`
			FirstRange  *rangeDescriptorInfo `json:"first-range"`
			Permissions *prefixedInfo        `json:"permissions"`
			Zones       *prefixedInfo        `json:"zones"`
			ClusterID   *keyValueStringPair  `json:"cluster-id"`
			Node1       *keyValuePair        `json:"node:1"`
		} `json:"infos"`
	}

	httpClient, err := testContext.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	contentTypes := []string{"application/json", "application/x-protobuf", "text/yaml"}
	for _, contentType := range contentTypes {
		req, err := http.NewRequest("GET", testContext.RequestScheme()+"://"+s.ServingAddr()+statusGossipKeyPrefix, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Accept", contentType)

		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("unexpected status code: %v", resp.StatusCode)
		}
		returnedContentType := resp.Header.Get(util.ContentTypeHeader)
		if returnedContentType != "application/json" {
			t.Errorf("unexpected content type: %v", returnedContentType)
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		data := &infos{}
		if err = json.Unmarshal(body, &data); err != nil {
			t.Fatal(err)
		}
		if data.Infos.Accounting == nil {
			t.Errorf("no accounting info returned: %v,", body)
		}
		if data.Infos.FirstRange == nil {
			t.Errorf("no first-range info returned: %v", body)
		}
		if data.Infos.Permissions == nil {
			t.Errorf("no permission info returned: %v", body)
		}
		if data.Infos.Zones == nil {
			t.Errorf("no zone info returned: %v", body)
		}
		if data.Infos.ClusterID == nil {
			t.Errorf("no clusterID info returned: %v", body)
		}
		if data.Infos.Node1 == nil {
			t.Errorf("no node 1 info returned: %v", body)
		}
	}
}

// startServerAndGetStatus will startup a server with a short scan interval,
// wait for the scan to completed, fetch the request status based on the
// keyPrefix. The test server and fetched status are returned. The caller is
// responsible to stop the server.
func startServerAndGetStatus(t *testing.T, keyPrefix string) (*TestServer, []byte) {
	ts := &TestServer{}
	ts.Ctx = NewTestContext()
	ts.Ctx.ScanInterval = time.Duration(5 * time.Millisecond)
	ts.StoresPerNode = 3
	if err := ts.Start(); err != nil {
		t.Fatal(err)
	}

	// Make sure the node is spun up and that a full scan of the ranges in the
	// stores is complete.  The best way to do that is to wait twice.
	ts.node.waitForScanCompletion()
	ts.node.waitForScanCompletion()

	httpClient, err := testContext.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest("GET", testContext.RequestScheme()+"://"+ts.ServingAddr()+keyPrefix, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("unexpected status code: %v", resp.StatusCode)
	}
	returnedContentType := resp.Header.Get(util.ContentTypeHeader)
	if returnedContentType != "application/json" {
		t.Errorf("unexpected content type: %v", returnedContentType)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return ts, body
}

// TestNodeStatusResponse verifies that node status returns the expected
// results.
// TODO(Bram): Add more nodes.
func TestNodesStatusResponse(t *testing.T) {
	ts, body := startServerAndGetStatus(t, statusNodesKeyPrefix)
	defer ts.Stop()
	nodeStatuses := []proto.NodeStatus{}
	if err := json.Unmarshal(body, &nodeStatuses); err != nil {
		t.Fatal(err)
	}

	if len(nodeStatuses) != 1 {
		t.Errorf("too many node statuses returned - expected:1 actual:%d", len(nodeStatuses))
	}
	if !reflect.DeepEqual(ts.node.Descriptor, nodeStatuses[0].Desc) {
		t.Errorf("node status descriptors are not equal\nexpected:%+v\nactual:%+v\n", ts.node.Descriptor, nodeStatuses[0].Desc)
	}
}

// TestStoreStatusResponse verifies that node status returns the expected
// results.
// TODO(Bram): Add more nodes.
func TestStoresStatusResponse(t *testing.T) {
	ts, body := startServerAndGetStatus(t, statusStoresKeyPrefix)
	defer ts.Stop()
	storeStatuses := []proto.StoreStatus{}
	if err := json.Unmarshal(body, &storeStatuses); err != nil {
		t.Fatal(err)
	}

	if len(storeStatuses) != ts.node.lSender.GetStoreCount() {
		t.Errorf("too many node statuses returned - expected:%d, actual:%d", ts.node.lSender.GetStoreCount(), len(storeStatuses))
	}
	for _, storeStatus := range storeStatuses {
		storeID := storeStatus.Desc.StoreID
		store, err := ts.node.lSender.GetStore(storeID)
		if err != nil {
			t.Fatal(err)
		}
		desc, err := store.Descriptor()
		if err != nil {
			t.Fatal(err)
		}
		// The capacities fluctuate a lot, so drop them for the deep equal.
		desc.Capacity = proto.StoreCapacity{}
		storeStatus.Desc.Capacity = proto.StoreCapacity{}
		if !reflect.DeepEqual(*desc, storeStatus.Desc) {
			t.Errorf("store status descriptors are not equal\nexpected:%+v\nactual:%+v\n", *desc, storeStatus.Desc)
		}
	}
}
