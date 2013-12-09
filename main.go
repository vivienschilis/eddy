package main

import (
	"fmt"
	"github.com/zimbatm/httputil2"
	"html/template"
	"log"
	"net/http"
	"os"
	"time"
)

const homepageHtml = `
<html>
  <head>
    <script src="//ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>
  	<script>
		var source = new EventSource("/events?channels={{.Channels}}");
		source.addEventListener('message', function(e) {
		  console.log(e);
		  $("#messages").append("<p>" + e.data + "</p>");
		}, false);
  	</script>
  </head>
  <body>
		<form action="/send" method=GET>
			<input name=channel value=foo>
			<input name=data>
			<input type=submit>
		</form>
    Look at all of the messages from the server:
    <div id="messages"/>
  </body>
</html>
`

type HomepageParams struct {
	Channels string
}

var homepageTemplate = template.Must(template.New("home").Parse(homepageHtml))

func homePage(w http.ResponseWriter, req *http.Request) {
	channels := req.URL.Query().Get("channels")
	if len(channels) == 0 {
		channels = "foo,bar"
	}
	homepageTemplate.Execute(w, HomepageParams{channels})
}

func main() {
	var h http.Handler

	serverAddr := ":9001"
	redisAddr := ":6379"

	op := NewMultiplexer(redisAddr)

	mux := http.NewServeMux()
	mux.Handle("/events", NewSubscriberHandler(op))
	mux.HandleFunc("/", homePage)
	mux.Handle("/send", NewPublisherHandler(redisAddr))

	h = httputil2.GzipHandler(mux)
	h = httputil2.LogHandler(
		h,
		os.Stdout,
		httputil2.CommonLogFormatter(httputil2.CommonLogFormat),
	)

	s := &http.Server{
		Addr:           serverAddr,
		Handler:        h,
		ReadTimeout:    20 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	fmt.Printf("Starting app at %s\n", serverAddr)
	log.Fatal(s.ListenAndServe())
}
