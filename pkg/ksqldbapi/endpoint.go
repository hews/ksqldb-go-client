package ksqldbapi

import "net/url"

var (
	// EndpointStatusQuery is used to introspect query status.
	EndpointStatusQuery = newEndpoint("/status")

	// EndpointStatusServer is used to introspect server status.
	EndpointStatusServer = newEndpoint("/info")

	// EndpointRunStatement is used to execute a statement.
	EndpointRunStatement = newEndpoint("/ksql")

	// EndpointRunQuery is used to run a query.
	EndpointRunQuery = newEndpoint("/query")

	// EndpointRunStreamQuery is used to run push and pull queries.
	EndpointRunStreamQuery = newEndpoint("/query-stream")

	// EndpointTerminate is used to terminate a cluster.
	EndpointTerminate = newEndpoint("/ksql/terminate")
)

// Endpoint embeds and decorates a basic URL.
type Endpoint struct {
	*url.URL
}

// newEndpoint handles initialization logic for the list of endpoints.
func newEndpoint(path string) Endpoint {
	urlpath, _ := url.Parse(path)
	return Endpoint{URL: urlpath}
}

// On just reverses the roles for url.URL's ResolveReference(). Simple.
func (ep *Endpoint) On(host *url.URL) *url.URL {
	return host.ResolveReference(ep.URL)
}
