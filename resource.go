package ksqldb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"hews.co/ksqldb/pkg/ksqldbapi"
)

// DefaultHeaders are the default headers that will be attached to each
// Resource. They will be added to any generated HTTP request unless
// changed.
var DefaultHeaders = map[string]string{
	"Content-Type": "application/vnd.ksql.v1+json; charset=utf-8",
	"Accept":       "application/vnd.ksql.v1+json",
}

// Resource represents all the information necessary to describe a
// request in the KsqlDB REST API: the method, endpoint, body/payload,
// HTTP request headers, and API version.
type Resource struct {
	Payload    *Payload
	Endpoint   *ksqldbapi.Endpoint
	Method     string
	Headers    map[string]string
	APIVersion string
}

// Payload represents the JSON body sent as a KSQL statement or query to
// the server.
type Payload struct {
	Ksql  string            `json:"ksql"`
	Props map[string]string `json:"streamsProperties"`
	Seq   int64             `json:"commandSequenceNumber,omitempty"`
}

// NewStatement provisions a KSQL statement as a Resource.
func NewStatement(ksql string) Requester {
	return &Resource{
		Payload: &Payload{
			Ksql:  ksql,
			Props: make(map[string]string),
		},
		Endpoint:   &ksqldbapi.EndpointRunStatement,
		Method:     http.MethodPost,
		Headers:    DefaultHeaders,
		APIVersion: "v1",
	}
}

// NewQuery provisions a KSQL query (ie, a SELECT statement) as a
// Resource.
func NewQuery(ksql string) Requester {
	return &Resource{
		Payload: &Payload{
			Ksql:  ksql,
			Props: make(map[string]string),
		},
		Endpoint:   &ksqldbapi.EndpointRunQuery,
		Method:     http.MethodPost,
		Headers:    DefaultHeaders,
		APIVersion: "v1",
	}
}

// Requester implements a "request generator" that turns a KsqlDB REST
// API resource description and KSQL statement into a basic HTTP request.
type Requester interface {
	Request(serverURL *url.URL) (*http.Request, error)
	json.Marshaler
}

// createRequest does as it claims: it creates the HTTP request. It
// should be able to do all of this without any input outside of the
// resource object (except the server URL). It shouldn't need to know a
// thing about the client.
//
// TODO: [PJ] this will take into account the request, etc. As needed we
// can also add configuration that would get activated here.
func createRequest(method string, url string, payload *Payload, headers map[string]string) (*http.Request, error) {
	byt, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("ksql request: unmarshaling query: %w", err)
	}
	req, err := http.NewRequest(method, url, bytes.NewBuffer(byt))
	if err != nil {
		return nil, fmt.Errorf("ksql request: creating HTTP request: %w", err)
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	return req, nil
}

// MarshalJSON forwards request to marshal the resource to the payload.
//
// NOTE: [PJ] this will get weird: there are corner cases for different
// types of KSQL, or even based on the endpoint used. So enjoy this
// moment of calm.
func (rr *Resource) MarshalJSON() ([]byte, error) {
	return json.Marshal(rr.Payload)
}

// Request implements Requester on all resource objects. In essence, it
// resolves the API URL based on the given server URL and then forwards
// all of the resources information to the internal createRequest
// function.
func (rr *Resource) Request(serverURL *url.URL) (*http.Request, error) {
	return createRequest(
		rr.Method,
		rr.Endpoint.On(serverURL).String(),
		rr.Payload,
		rr.Headers,
	)
}
