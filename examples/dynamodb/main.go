package main

import (
	"context"
	"fmt"
	"log"

	"github.com/savaki/eventsource"
	"github.com/savaki/eventsource/provider/dynamodbstore"
)

// User represents out domain model
type User struct {
	ID    string `eventsource:"id"`
	First string
	Last  string
}

// UserFirstSet defines an event by simple struct embedding
type UserFirstSet struct {
	eventsource.Model
	First string
}

// UserLastSet defines an event via tags
type UserLastSet struct {
	ID      string `eventsource:"id,type:user-last-set"`
	Version int    `eventsource:"version"`
	Last    string
}

func SetFirst(_ context.Context, aggregate, event interface{}) error {
	user := aggregate.(*User)
	v := event.(UserFirstSet)

	user.First = v.First
	return nil
}

func SetLast(_ context.Context, aggregate, event interface{}) error {
	user := aggregate.(*User)
	v := event.(UserLastSet)

	user.Last = v.Last
	return nil
}

func main() {
	store, err := dynamodbstore.New("user_events", dynamodbstore.WithRegion("us-west-2"))
	if err != nil {
		log.Fatalln(err)
	}

	userEvents := eventsource.New(User{}, eventsource.WithStore(store))
	userEvents.BindFunc(UserFirstSet{}, SetFirst)
	userEvents.BindFunc(UserLastSet{}, SetLast)

	id := "123"
	setFirstEvent := UserFirstSet{
		Model: eventsource.Model{
			AggregateID: id,
			Version:     1,
		},
		First: "Joe",
	}
	setLastEvent := UserLastSet{
		ID:      id,
		Version: 2,
		Last:    "Public",
	}

	ctx := context.Background()
	err = userEvents.Save(ctx, setFirstEvent, setLastEvent)
	if err != nil {
		log.Fatalln(err)
	}

	v, version, err := userEvents.Load(ctx, id)
	if err != nil {
		log.Fatalln(err)
	}

	user := v.(*User)
	fmt.Printf("Hello %v %v [Version %v]\n", user.First, user.Last, version) // prints "Hello Joe Public [1]"
}
