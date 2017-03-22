package main

import (
	"context"
	"fmt"
	"log"

	"github.com/savaki/eventsource"
	"github.com/savaki/eventsource/provider/dynamodbstore"
)

// UserCreated defines a user creation event
type UserCreated struct {
	eventsource.Model
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

type User struct {
	ID      string
	Version int
	Name    string
	Email   string
}

func (item *User) On(event interface{}) bool {
	switch v := event.(type) {
	case *UserCreated:
		item.Version = v.Model.Version
		item.ID = v.Model.ID

	case *UserNameSet:
		item.Version = v.Model.Version
		item.Name = v.Name

	case *UserEmailSet:
		item.Version = v.Version
		item.Email = v.Email

	default:
		return false
	}

	return true
}

func main() {
	store, err := dynamodbstore.New("user_events", dynamodbstore.WithRegion("us-west-2"))
	if err != nil {
		log.Fatalln(err)
	}

	userEvents := eventsource.New(&User{}, eventsource.WithStore(store))
	err = userEvents.Bind(
		UserCreated{},
		UserNameSet{},
		UserEmailSet{},
	)
	if err != nil {
		log.Fatalln(err)
	}

	id := "123"
	setNameEvent := &UserNameSet{
		Model: eventsource.Model{ID: id, Version: 1},
		Name:  "Joe Public",
	}
	setEmailEvent := &UserEmailSet{
		ID:      id,
		Version: 2,
		Email:   "joe.public@example.com",
	}

	ctx := context.Background()
	err = userEvents.Save(ctx, setEmailEvent, setNameEvent)
	if err != nil {
		log.Fatalln(err)
	}

	v, err := userEvents.Load(ctx, id)
	if err != nil {
		log.Fatalln(err)
	}

	user := v.(*User)
	fmt.Printf("Hello %v %v\n", user.Name, user.Email) // prints "Hello Joe Public joe.public@example.com"
}
