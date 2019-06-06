FROM alpine:3.8

RUN apk upgrade --update --no-cache
RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix
WORKDIR /home/atomix

ADD build/atomixctl/_output/bin/atomixctl /usr/local/bin/atomixctl
