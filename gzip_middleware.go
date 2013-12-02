package main

// Stolen from https://github.com/codegangsta/martini-contrib/blob/master/gzip/gzip.go

import (
	"compress/gzip"
	"net/http"
	"strings"
)

const (
	HeaderAcceptEncoding  = "Accept-Encoding"
	HeaderContentEncoding = "Content-Encoding"
	HeaderContentLength   = "Content-Length"
	HeaderContentType     = "Content-Type"
	HeaderVary            = "Vary"
)

func GzipMiddleware(parent http.Handler) http.Handler {
	return &gzipMiddleware{parent}
}

type gzipMiddleware struct {
	parent http.Handler
}

func (self *gzipMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Tell proxies that the content might vary
	w.Header().Add(HeaderVary, HeaderAcceptEncoding)

	// Ignore the client if it doesn't support the gzip content encoding
	if !strings.Contains(r.Header.Get(HeaderAcceptEncoding), "gzip") {
		self.parent.ServeHTTP(w, r)
		return
	}

	f, f_ok := w.(http.Flusher)
	if !f_ok {
		panic("ResponseWriter is not a Flusher")
	}

	cn, cn_ok := w.(http.CloseNotifier)
	if !cn_ok {
		panic("ResponseWriter is not a CloseNotifier")
	}

	//
	w.Header().Set(HeaderContentEncoding, "gzip")
	gz := gzip.NewWriter(w)
	gzw := &gzipResponseWriter{gz, w, f, cn}

	self.parent.ServeHTTP(gzw, r)
}

type gzipResponseWriter struct {
	gz *gzip.Writer
	w  http.ResponseWriter
	f  http.Flusher
	cn http.CloseNotifier
}

func (self *gzipResponseWriter) Header() http.Header {
	return self.w.Header()
}

func (self *gzipResponseWriter) WriteHeader(status int) {
	// Content-Length is wrong once compressed !
	self.Header().Del(HeaderContentLength)
	self.w.WriteHeader(status)
}

func (self *gzipResponseWriter) Write(p []byte) (int, error) {
	// Make sure to detect the content-type before we encode it
	// TODO: This should be done upstream instead
	if len(self.Header().Get(HeaderContentType)) == 0 {
		self.Header().Set(HeaderContentType, http.DetectContentType(p))
	}
	// Content-Length is wrong once compressed !
	self.Header().Del(HeaderContentLength)

	return self.gz.Write(p)
}

// For the http.Flusher interface. The server fails without.
func (self *gzipResponseWriter) Flush() {
	err := self.gz.Flush()
	// FIXME: How to deal with the error here ?
	if err != nil {
		panic(err)
	}
	self.f.Flush()
}

func (self *gzipResponseWriter) Close() error {
	err := self.gz.Close()
	if err != nil {
		return err
	}
	//self.w.Close()
	return nil
}

// For the the http.CloseNotifier interface. The server fails without.
func (self *gzipResponseWriter) CloseNotify() <-chan bool {
	c := make(chan bool)
	pc := self.cn.CloseNotify()

	go func() {
		<-pc
		self.Close()
		c <- true
	}()

	return c
}
