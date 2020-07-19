package ksqldb

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"net/url"
)

// Client is the top-level interface to the KsqlDB REST API. It handles
// queries and other RPCs and forwards them to the correct URL, using
// the most correct transport layer. It also manages connection
// (TCP or TLS) issues and pooling/multiplexing.
//
// Clients are structured for immutable(-ish) configuration: all their
// attributes are only accessible by getters (at most). While the
// underlying structures can be mutated, this is meant to reduce the
// chance that a program would not act from request-to-request in a
// reliable way.
type Client struct {
	ctx        context.Context
	serverURL  *url.URL
	httpClient *http.Client
	httpTrace  *ClientTrace
}

// ClientOptions are the parameters that may be passed when
// instantiating a new client.
//
// TODO: [PJ] gotta add a logger!
type ClientOptions struct {
	URL     string
	Trace   *ClientTrace
	Context context.Context
}

// ClientTrace extends httptrace.ClientTrace with two final hooks, for
// simplicity and completeness' sake: RequestPrepared and
// ResponseDelivered. These hooks decorate the call to http.RoundTripper
// and therefore are unnecessary to enact in plumbing: however since we
// are wrapping all client interactions it makes for a simpler interface
// for instrumenting tracing, logging or other telemetry / debugging.
//
// TODO: [PJ] wrap the entire interface, proxying as necessary, so that
// every hook is passed a copy of the client (to access the logger,
// client metadata, etc)
type ClientTrace struct {
	// See https://golang.org/pkg/net/http/httptrace/#ClientTrace.
	*httptrace.ClientTrace

	// RequestPrepared is passed the request generated from the incoming
	// resource. Andy client mutations take place after this is called. The
	// request is pristine.
	RequestPrepared func(*http.Request)

	// ResponseDelivered instead of ResponseDone or etc, because quite
	// often the response is streaming (HTTP/1.1 or HTTP/2) and therefore
	// the connection continues and the response may not be done writing:
	// however, at this moment the response header and status have been
	// delivered and therefore the status of the request can be determined.
	ResponseDelivered func(*http.Response, error)
}

// newTransportFromDefault clones the default transport. Why change it?
// I can't say – people just liked it better that way?
func newTransportFromDefault() *http.Transport {
	return http.DefaultTransport.(*http.Transport).Clone()
}

// parseServerURL parses and validates the given server URL string.
func parseServerURL(rawURL string) (*url.URL, error) {
	uu, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if uu.Scheme == "" {
		return nil, fmt.Errorf("url %s missing scheme", rawURL)
	}
	if uu.Path != "" && uu.Path != "/" {
		return nil, fmt.Errorf("url %s should not contain path", rawURL)
	}
	return uu, nil
}

// NewClient creates a new KsqlDB client handler for the server located
// at the given URL. The URL must be a complete path, including scheme.
func NewClient(opts ClientOptions) (*Client, error) {
	transport := newTransportFromDefault()

	// FIXME: [PJ] for the current streaming setup, it makes a lot more
	// sense to force uncompressed transport and then scan directly on
	// the incoming reader. Should move to a system that pipes through
	// decompression and then scans.
	transport.DisableCompression = true

	serverURL, err := parseServerURL(opts.URL)
	if err != nil {
		return nil, fmt.Errorf("initializing ksqldb client: %w", err)
	}

	httpClient := &http.Client{Transport: transport}
	cc := &Client{
		serverURL:  serverURL,
		httpClient: httpClient,
		httpTrace:  opts.Trace,
	}
	if opts.Context == nil {
		cc.ctx = context.Background()
	} else {
		cc.ctx = opts.Context
	}

	return cc, nil
}

// ServerURL gets the private attribute. Not allowing sets here helps
// keep the client configuration immutable.
func (cc *Client) ServerURL() *url.URL {
	return cc.serverURL
}

// HTTPClient gets the private attribute. Not allowing sets here helps
// keep the client configuration immutable.
func (cc *Client) HTTPClient() *http.Client {
	return cc.httpClient
}

// HTTPTrace gets the private attribute. Not allowing sets here helps
// keep the client configuration immutable.
func (cc *Client) HTTPTrace() *ClientTrace {
	return cc.httpTrace
}

// WithClientConfig runs on every query, attaching the context (see
// client.Do: the passed context is a cancelable child of the client's
// context) and any configured tracing to the request. This allows full
// control and instrumentation of client requests.
//
// TODO: [PJ] this may need to take into account the request, etc. As
// needed we can also add configuration at the client level that would
// be activated here.
func (cc *Client) WithClientConfig(ctx context.Context, req *http.Request) *http.Request {
	trace := cc.HTTPTrace()
	if trace != nil && trace.ClientTrace != nil {
		return req.WithContext(httptrace.WithClientTrace(ctx, trace.ClientTrace))
	}
	return req.WithContext(ctx)
}

// Do handles the client logic around performing a request. It wraps the
// underlying HTTP client (and its call to httpClient.Do), adding or
// implementing all necessary tracing and context management, and error
// handling.
//
// Do should be passed a resource object that can generate its own
// HTTP request with the sole input of the server's URL. All the output
// is bundled together on return as a KsqlDB Response.
//
// TODO: [PJ] allow setting a deadline or timeout for the request's
// context.
func (cc *Client) Do(resource Requester) (*Response, error) {
	req, err := resource.Request(cc.serverURL)
	if err != nil {
		return nil, fmt.Errorf("sending ksql request: %w", err)
	}
	ctx, cancel := context.WithCancel(cc.ctx)
	trace := cc.HTTPTrace()
	if trace != nil && trace.RequestPrepared != nil {
		trace.RequestPrepared(req)
	}
	resp, err := cc.httpClient.Do(cc.WithClientConfig(ctx, req))
	if trace != nil && trace.ResponseDelivered != nil {
		trace.ResponseDelivered(resp, err)
	}
	if err != nil {
		// Avoiding a lost cancel.
		return &Response{cancelFunc: cancel}, fmt.Errorf("sending ksql request: %w", err)
	}
	return &Response{
		Response:   resp,
		Context:    ctx,
		cancelFunc: cancel,
	}, nil
}
