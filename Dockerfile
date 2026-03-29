FROM alpine:3.21
ARG TARGETPLATFORM
RUN apk add --no-cache ca-certificates
COPY ${TARGETPLATFORM}/gomq ${TARGETPLATFORM}/gomqperf /usr/local/bin/
ENTRYPOINT ["gomq"]
