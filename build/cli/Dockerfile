FROM alpine:3.8

RUN apk upgrade --update --no-cache

USER nobody

ADD build/cli/_output/bin/atomix-cli /usr/local/bin/atomix-cli
