package main

import (
	"eventsource"
	"fmt"
	"html/template"
	"net/http"
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

func eventHandler(es *eventsource.Conn, channels []string) {
	sc, err := NewSubscriberConnection(channels, es)
	if err != nil {
		es.Write(err.Error())
		return
	}

	defer sc.Close()

	fmt.Println("Client connected.")

	sc.Listen()

	fmt.Println("Client went away.")
	return
}

func homePage(w http.ResponseWriter, req *http.Request) {
	channels := req.URL.Query().Get("channels")
	homepageTemplate.Execute(w, HomepageParams{channels})
}

func eventSend(w http.ResponseWriter, req *http.Request) {
	channel := req.URL.Query().Get("channel")
	data := req.URL.Query().Get("data")

	eventPublisher.c <- Event{channel, data}
	w.WriteHeader(201)
}

func main() {
	serverAddress := ":9001"
	fmt.Printf("Starting app at %s\n", serverAddress)
	http.Handle("/events", GzipMiddleware(eventsource.Handler(eventHandler)))
	http.HandleFunc("/", homePage)
	http.HandleFunc("/send", eventSend)
	http.ListenAndServe(serverAddress, nil)
}
