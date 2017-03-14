package main

import (
	"context"
	"fmt"
	"log"

	"github.com/savaki/eventsource"
)

type User struct {
	ID    string `eventsource:"id"`
	Name  string
	Email string
}

// UserFirstSet defines an event by simple struct embedding
type UserNameSet struct {
	eventsource.Model
	Name string
}

// UserLastSet defines an event via tags
type UserEmailSet struct {
	ID      string `eventsource:"id,type:user-last-set"`
	Version int    `eventsource:"version"`
	Email   string
}

func SetName(_ context.Context, aggregate, event interface{}) error {
	user := aggregate.(*User)
	v := event.(*UserNameSet)

	user.Name = v.Name
	return nil
}

func SetEmail(_ context.Context, aggregate, event interface{}) error {
	user := aggregate.(*User)
	v := event.(*UserEmailSet)

	user.Email = v.Email
	return nil
}

func main() {
	userEvents := eventsource.New(User{})
	userEvents.BindFunc(UserNameSet{}, SetName)
	userEvents.BindFunc(UserEmailSet{}, SetEmail)

	id := "123"
	setFirstEvent := UserNameSet{
		Model: eventsource.Model{
			ID:      id,
			Version: 1,
		},
		Name: "Joe Public",
	}
	setLastEvent := UserEmailSet{
		ID:      id,
		Version: 2,
		Email:   "joe.public@example.com",
	}

	ctx := context.Background()
	err := userEvents.Save(ctx, setFirstEvent, setLastEvent)
	if err != nil {
		log.Fatalln(err)
	}

	v, version, err := userEvents.Load(ctx, id)
	if err != nil {
		log.Fatalln(err)
	}

	user := v.(*User)
	fmt.Printf("Hello %v %v [Version %v]\n", user.Name, user.Email, version) // prints "Hello Joe Public joe.public@example.com [Version 2]"
}
