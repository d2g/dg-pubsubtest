package dgpubsub

import (
	"encoding/base64"
	"fmt"
	"net/http"

	//"cloud.google.com/go/pubsub"

	"golang.org/x/oauth2/google"

	pubsub "google.golang.org/api/pubsub/v1"

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
	hc, err := google.DefaultClient(ctx, pubsub.PubsubScope)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error Getting Pubsub Client: %s", err), http.StatusInternalServerError)
		return
	}

	ps, err := pubsub.New(hc)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error Getting Pubsub Service: %s", err), http.StatusInternalServerError)
		return
	}

	pr, err := ps.Projects.Topics.Publish(
		"projects/dg-pubsubtest/topics/example",
		&pubsub.PublishRequest{
			Messages: []*pubsub.PubsubMessage{
				{
					Attributes: map[string]string{
						"ATTR1": "Yes",
						"ATTR2": "true",
					},
					Data: base64.StdEncoding.EncodeToString([]byte("hello")),
				},
			},
		},
	).Do()
	if err != nil {
		http.Error(w, fmt.Sprintf("Error Publishing: %s", err), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Publish Result: %v\n", pr)
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
