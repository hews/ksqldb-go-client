package ksqldb

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// // DefaultMaxReadBuffer represents the default size of the read buffer
// // we pipe our response body into.
// var DefaultMaxReadBuffer = 1024 * 1024

// Response bundles the various data needed to parse a KsqlDB REST API
// response.
type Response struct {
	*http.Response
	Context    context.Context
	cancelFunc context.CancelFunc
	once       sync.Once
	dataCh     chan []byte
	errCh      chan error
}

// Cancel cancels the response's context.
func (rr *Response) Cancel() {
	rr.cancelFunc()
}

// Read initializes reading (setting up the channels, starts reading the
// response into them) and returns the data and error channels. All
// other readers must call this in order to get read the response.
func (rr *Response) Read() (<-chan []byte, <-chan error) {
	rr.once.Do(rr.initAsyncRead)
	return rr.dataCh, rr.errCh
}

// apiDataDelimiter is just a bytes-comparable representation of the
// delimiter for streaming records. In the v1 JSON API that is \n.
//
// TODO: [PJ] on the scanners below, we should scan for this delimiter!
var apiDataDelimiter = []byte("\n")

// filterSendDataChannel checks incoming byte arrays for meaningful data
// to send on the channel.
func filterSendDataChannel(dataCh chan<- []byte, byt []byte) {
	if byt != nil && len(byt) != 0 && !bytes.Equal(byt, apiDataDelimiter) {
		dataCh <- byt
	}
}

// initAsyncRead reads the HTTP response body into some channels, for
// the caller to consume at their leisure.
//
// FIXME: [PJ] using an unbuffered data channel here opens up a class of
// problems. Should probably use a buffered channel and/or pipe the
// response body to a buffer with a hard capacity that can trigger a
// cancellation on overflow.
//
// TODO: [PJ] we are here assuming a readable newline must be met along
// the way, otherwise we get stuck in IO blocking foreaver. This is why
// we are forcing uncompressed transmission (I think*) and should be
// rectified. ALSO, it is a little brittle: should handle reading on a
// byte slice / buffer and fail meaningfully if there is a mismatch in
// purported content type and actual.
//
// * – it's possible the server doesn't support it and returns 200 and
// just hangs on an open connection, but I truly doubt it. I just
// haven't verified.
func (rr *Response) initAsyncRead() {
	rr.dataCh = make(chan []byte)
	rr.errCh = make(chan error)

	scanner := bufio.NewScanner(rr.Response.Body)
	go func(dataCh chan<- []byte, errCh chan<- error) {
		for {
			select {
			case <-rr.Context.Done():
				errCh <- context.Canceled
				close(dataCh)
				close(errCh)
				return
			default:
				if ok := scanner.Scan(); !ok {
					// QUESTION: [PJ] is it possible in HTTP/2 to
					// encounter an error here that is recoverable?
					if err := scanner.Err(); err == nil {
						errCh <- io.EOF
					} else {
						errCh <- err
					}
					filterSendDataChannel(dataCh, scanner.Bytes())
					close(dataCh)
					close(errCh)
					return
				}
				filterSendDataChannel(dataCh, scanner.Bytes())
			}
		}
	}(rr.dataCh, rr.errCh)
}

// newBuffer is a utility to increase code redability and reduce code
// duplication.
func newBuffer() *bytes.Buffer {
	return bytes.NewBuffer(make([]byte, bytes.MinRead))
}

// writeToBuffer is a utility to increase code redability and reduce
// code duplication.
func writeToBuffer(byt []byte, buf *bytes.Buffer) error {
	_, err := buf.Write(byt)
	if err != nil {
		err = fmt.Errorf("writing response into buffer: %w", err)
	}
	return err
}

// drainDataToBytes is a utility to increase code redability and reduce
// code duplication.
func drainDataToBytes(dataCh <-chan []byte) ([]byte, error) {
	var err error

	buf := newBuffer()
	for byt := range dataCh {
		err = writeToBuffer(byt, buf)
	}
	return buf.Bytes(), err
}

// isOneOf is a utility to increase code redability and reduce code
// duplication.
func isOneOf(e error, errs []error) bool {
	for _, err := range errs {
		if errors.Is(e, err) {
			return true
		}
	}
	return false
}

// ReadStreaming scans the incoming data and passes it to a callback.
// The streaming API delimits records clearly, so those can be used to
// build an interface for parsing streaming data into actionable records.
//
// The handlers should be complex, since they are also in charge of all
// error handling and management. In essence, the handler will receive
// all of the available values from the data and error channels, and
// must act accordingly. Returing false from the handler will cancel the
// context and abort stream reading; any error will also abort the
// stream after some draining (complex logic...) TKTKTK
func (rr *Response) ReadStreaming(handler func([]byte) error) error {
	var byt []byte

	dataCh, errCh := rr.Read()
	for {
		select {
		case byt = <-dataCh:
			if err := handler(byt); err != nil {
				rr.Cancel()
				return err
			}
		case err := <-errCh:
			// Recoverable errors (EOF or context canceling)should
			// trigger a drain on the data channel, an end to which can
			// be ensured by canceling the context first.
			rr.Cancel()

			// Prioritize any errors that arise in the handler while
			// draining the data channel over the recoverable errors.
			if isOneOf(err, []error{io.EOF, context.Canceled, context.DeadlineExceeded}) {
				byt, derr := drainDataToBytes(dataCh)
				if derr != nil {
					return derr
				}
				if herr := handler(byt); herr != nil {
					return herr
				}
				if errors.Is(err, io.EOF) {
					return nil
				}
				return fmt.Errorf("reading response body: %w", err)
			}
			return fmt.Errorf("reading response body: %w", err)
		}
	}
}

// ReadAll foolishly blocks on reading the entire response before
// returning the buffered output. This is the simplest way to handle
// the response (well, I mean, other than ioutil.ReadAll()).
func (rr *Response) ReadAll() ([]byte, error) {
	buf := newBuffer()
	serr := rr.ReadStreaming(func(byt []byte) error {
		return writeToBuffer(byt, buf)
	})
	return buf.Bytes(), serr
}
