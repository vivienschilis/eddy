package main

import (
	"fmt"
	"html/template"
	"log"
	"net/http"
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
	homepageTemplate.Execute(w, HomepageParams{channels})
}

func eventSend(w http.ResponseWriter, req *http.Request) {
	channel := req.URL.Query().Get("channel")
	data := req.URL.Query().Get("data")
	timestamp := time.Now().UnixNano()

	eventPublisher.q <- &Event{timestamp, channel, data}
	w.WriteHeader(201)
}

func main() {
	serverAddress := ":9001"
	redisAddr := ":6379"

	op := NewMultiplexer(redisAddr)

	mux := http.NewServeMux()
	mux.Handle("/events", NewSubscriberHandler(op))
	mux.HandleFunc("/", homePage)
	mux.HandleFunc("/send", eventSend)

	s := &http.Server{
		Addr:           serverAddress,
		Handler:        mux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	fmt.Printf("Starting app at %s\n", serverAddress)
	log.Fatal(s.ListenAndServe())
}
