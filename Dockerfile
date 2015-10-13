FROM gliderlabs/alpine:3.2
ENTRYPOINT ["dagger"]

COPY . /go/src/github.com/nsaje/dagger
RUN apk-install -t build-deps go git \
	&& cd /go/src/github.com/nsaje/dagger \
	&& export GOPATH=/go \
	&& go get \
	&& go install ./... \
	&& rm -rf /go \
	&& apk del --purge build-deps
