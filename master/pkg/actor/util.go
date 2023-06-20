package actor

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"
)

// Responses wraps a collection of response objects from different actors.
type Responses <-chan Response

// MarshalJSON implements the json.Marshaler interface.
func (r Responses) MarshalJSON() ([]byte, error) {
	responses := r.GetAll()
	results := make(map[string]Message, len(responses))
	for source, response := range responses {
		results[source.Address().String()] = response
	}
	return json.Marshal(results)
}

// GetAll waits for all actors to respond and returns a mapping of all actors and their
// corresponding responses.
func (r Responses) GetAll() map[*Ref]Message {
	results := make(map[*Ref]Message, cap(r))
	for response := range r {
		if !response.Empty() {
			results[response.Source()] = response.Get()
		}
	}
	return results
}

func askAll(
	ctx context.Context, message Message, timeout *time.Duration, sender *Ref, actors []*Ref,
) Responses {
	results := make(chan Response, len(actors))
	wg := sync.WaitGroup{}
	wg.Add(len(actors))
	for _, actor := range actors {
		resp := actor.ask(ctx, sender, message)
		go func() {
			defer wg.Done()
			// Wait for the response to be ready before putting into the result channel.
			if timeout == nil {
				resp.Get()
			} else {
				resp.GetOrTimeout(*timeout)
			}
			results <- resp
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	return results
}

type (
	InvalidRequest     struct{ error }
	ActorNotFound      struct{ error }
	NoResponse         struct{ error }
	ErrorResponse      struct{ error }
	UnexpectedResponse struct{ error }
)

func (e ErrorResponse) Unwrap() error { return e.error }

// AskFunc asks an actor and sets the response in v. It returns an error if the actor doesn't
// respond, respond with an error, or v isn't settable.
func AskFunc(ask func() Response, id string, v interface{}) error {
	if reflect.ValueOf(v).IsValid() && !reflect.ValueOf(v).Elem().CanSet() {
		return InvalidRequest{
			fmt.Errorf("ask %s has valid but unsettable resp %T", id, v),
		}
	}
	expectingResponse := reflect.ValueOf(v).IsValid() && reflect.ValueOf(v).Elem().CanSet()
	switch resp := ask(); {
	case resp.Source() == nil:
		return ActorNotFound{
			fmt.Errorf("actor %s could not be found", id),
		}
	case expectingResponse && resp.Empty(), expectingResponse && resp.Get() == nil:
		return NoResponse{
			fmt.Errorf("actor %s did not respond", id),
		}
	case resp.Error() != nil:
		return ErrorResponse{
			resp.Error(),
		}
	default:
		if expectingResponse {
			if reflect.ValueOf(v).Elem().Type() != reflect.ValueOf(resp.Get()).Type() {
				return UnexpectedResponse{
					fmt.Errorf("%s returned unexpected resp (%T): %v", id, resp, resp),
				}
			}
			reflect.ValueOf(v).Elem().Set(reflect.ValueOf(resp.Get()))
		}
		return nil
	}
}
