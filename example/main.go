package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"net/http/httputil"
	"strconv"
	"strings"
	"sync"
	"time"

	"hews.co/ksqldb"
)

func logRequest(req *http.Request) {
	byt, err := httputil.DumpRequest(req, true)
	if err != nil {
		panic(err)
	}
	fmt.Print("\n> " + string(bytes.ReplaceAll(byt, []byte("\n"), []byte("\n> "))))
}

func logResponse(resp *http.Response, err error) {
	if err != nil {
		panic(err)
	}
	byt, err := httputil.DumpResponse(resp, false)
	if err != nil {
		panic(err)
	}
	fmt.Print("\n< " + string(bytes.ReplaceAll(byt, []byte("\n"), []byte("\n< "))))
}

func main() {
	// This example runs a few simple statements: creating, listing, and
	// then destroying a stream associated with a topic. It uses the
	// tracing mechanism to log the interactions clearly to stdout.
	client, err := ksqldb.NewClient(ksqldb.ClientOptions{
		URL: "http://0.0.0.0:8088",
		Trace: &ksqldb.ClientTrace{
			RequestPrepared:   logRequest,
			ResponseDelivered: logResponse,
			ClientTrace: &httptrace.ClientTrace{
				GotConn: func(connInfo httptrace.GotConnInfo) {
					fmt.Printf("\n> Connection established: %+v\n", connInfo)
				},
			},
		},
	})
	if err != nil {
		panic(err)
	}

	statements := []string{
		`CREATE STREAM
		transactions (
			accountID BIGINT,
			marketID  BIGINT,
			amount    BIGINT,
			unit      VARCHAR
		)
		WITH (
			KAFKA_TOPIC  = 'transactions',
			VALUE_FORMAT = 'JSON',
			PARTITIONS   = 1
		);`,
		`SHOW STREAMS;`,
		// NOTE: [PJ] leave for below `DROP STREAM transactions;`,
	}

	for _, statement := range statements {
		rr := ksqldb.NewStatement(strings.Join(strings.Fields(statement), " "))
		rh, err := client.Do(rr)
		if err != nil {
			panic(err)
		}
		byt, err := rh.ReadAll()
		if err != nil {
			panic(err)
		}
		fmt.Println(string(byt))
	}

	// This example shows a streaming query, sending new records while
	// the query is going. It also shows setting a client context that
	// times out: not really important (per-request timeouts would be,
	// but are not implemented) but useful here.
	fmt.Println("\n> STREAMING EXAMPLE:")
	ctx, _ := context.WithTimeout(context.Background(), 6*time.Second)
	client, err = ksqldb.NewClient(ksqldb.ClientOptions{
		URL:     "http://0.0.0.0:8088",
		Context: ctx,
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 1; i <= 3; i++ {
			time.Sleep(time.Second)
			wg.Wait() // Ensure we've opened the connection first.
			rr := ksqldb.NewStatement(
				`INSERT INTO transactions (accountID, marketID, amount, unit)
				VALUES (456, 22210, ` + strconv.Itoa(i) + `, 'USD');`,
			)
			_, err := client.Do(rr)
			if err != nil {
				panic(err)
			}
		}
	}()

	rr := ksqldb.NewQuery("SELECT * FROM transactions EMIT CHANGES;")
	rh, err := client.Do(rr)
	if err != nil {
		panic(err)
	}
	wg.Done()
	err = rh.ReadStreaming(func(byt []byte) error {
		fmt.Println("<< " + string(byt))
		return nil
	})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		panic(err)
	} else {
		fmt.Println("\nEt voilà!")
	}
}
