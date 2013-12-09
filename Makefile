export GOPATH=$(PWD)

eddy: *.go
	go build -o $@
