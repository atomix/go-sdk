FROM alpine:3.8

RUN apk upgrade --update --no-cache && apk add bash bash-completion
RUN addgroup -S atomix && adduser -S -G atomix atomix

USER atomix
WORKDIR /home/atomix

ADD build/atomixctl/_output/bin/atomixctl /usr/local/bin/atomixctl

RUN atomixctl init && \
    cp /etc/profile /home/atomix/.bashrc && \
    atomixctl completion bash > /home/atomix/.atomix/bash_completion.sh && \
    echo "source /home/atomix/.atomix/bash_completion.sh" >> /home/atomix/.bashrc

ENTRYPOINT ["/bin/bash"]
