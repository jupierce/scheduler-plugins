FROM golang:1.17
WORKDIR /go/src/github.com/kubernetes-sigs/scheduler-plugins
COPY . .
RUN make clean
RUN make
FROM registry.access.redhat.com/ubi7
COPY --from=0 /go/src/github.com/kubernetes-sigs/scheduler-plugins/bin/kube-scheduler /bin/kube-scheduler
RUN ["chmod", "+x", "/bin/kube-scheduler"]
CMD ["/bin/kube-scheduler"]
