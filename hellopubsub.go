package dgpubsub

import (
	"fmt"
	"net/http"

	"cloud.google.com/go/pubsub"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

// init is called before the application starts.
func init() {
	// Register a handler for /hello URLs.

	//Just to stop favicon triggering the push message functions
	http.HandleFunc("/favicon.ico", callback)

	http.HandleFunc("/callback", callback)
	http.HandleFunc("/addtask", addtask)
	http.HandleFunc("/_ah/queue/taskexample", hello)
	http.HandleFunc("/", hello)
}

func hello(w http.ResponseWriter, r *http.Request) {

	ctx := appengine.NewContext(r)
	log.Debugf(ctx, "Context Got")

	log.Debugf(ctx, "Creating PUBSUB Client")
	pc, err := pubsub.NewClient(ctx, appengine.RequestID(ctx))
	if err != nil {
		http.Error(w, fmt.Sprintf("Error Getting Pubsub Client: %s", err), http.StatusInternalServerError)
		return
	}
	log.Debugf(ctx, "PUBSUB Client Got")

	log.Debugf(ctx, "Getting Topic")
	tpc := pc.Topic("example")
	tpc.PublishSettings.CountThreshold = 0
	log.Debugf(ctx, "Topic Got")

	log.Debugf(ctx, "Creating Message")
	msg := &pubsub.Message{
		Data: []byte("Hello"),
	}

	log.Debugf(ctx, "Publishing Message")
	pr := tpc.Publish(ctx, msg)
	log.Debugf(ctx, "Message Published")

	//So we use PR
	log.Debugf(ctx, "PR: %v", pr)

	log.Debugf(ctx, "Awaiting Response")

	id, err := pr.Get(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error Getting Message Details: %v", err), http.StatusInternalServerError)
		return
	}
	log.Debugf(ctx, "Got Message ID")

	fmt.Fprintf(w, "Message ID: %v\n", id)
	log.Debugf(ctx, "Stopping Topic")
	tpc.Stop()
	log.Debugf(ctx, "Topic Stopped, closing client")
	pc.Close()
	log.Debugf(ctx, "Client Closed")
}

//Registered as where the pub sub messages get delivered so we can see the request the message generates.
func callback(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Nothing to See.")
}

//Try it in a TaskQueue
func addtask(w http.ResponseWriter, r *http.Request) {

	ctx := appengine.NewContext(r)

	t := &taskqueue.Task{}
	t.Method = "POST"
	id, err := taskqueue.Add(ctx, t, "taskexample")

	if err != nil {
		http.Error(w, fmt.Sprintf("Error Adding Task: %v", err), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "task: %v", id)
}
