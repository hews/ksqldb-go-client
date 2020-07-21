# KsqlDB Client (Golang)
## WIP

More to include. Right now, to get `example/main.go` to run, clone
the Confluent "all-in-one" KsqlDB Docker Compose repo, and run:

```
$ git clone https://github.com/confluentinc/cp-all-in-one
$ cd cp-all-in-one/cp-all-in-one
$ git checkout 5.5.1-post
$ docker-compose up
```

Then this should work:

```
$ go run example/main.go
```

Next steps: add tests, lock down basic transport functionality for
HTTP/1.1, uncompressed. Then build out resources vertically from the
bottom up: result type(s) and marshaller, client wrapper, KSQL builder.
Only marginally finish statements/queries: focus on status, healthcheck.

Implement for protobuf schemas.

Create a local setup to allow TLS connections (self-signed) and then
test drive HTTP/2 support against it. Pipe compressed streams through
decompression to allow streaming and gzip.

Copyright © 2020 Philip Hughes <p@hews.co>
