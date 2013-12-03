export GOPATH=$(PWD)

edvents: *.go
	go build -o $@
