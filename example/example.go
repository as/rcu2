package main

import (
	"fmt"

	"github.com/as/rcu2"
)

type Config struct {
	Zone string
	Service
}
type Service struct {
	ID   string
	Name string
}

func main() {
	s := rcu2.New()
	conf := [...]Config{
		{Zone: "alpha", Service: Service{ID: "1", Name: "research"}},
		{Zone: "alpha", Service: Service{ID: "2", Name: "development"}},
		{Zone: "bravo", Service: Service{ID: "3", Name: "shipping"}},
		{Zone: "bravo", Service: Service{ID: "4", Name: "recieving"}},
	}
	for i, v := range conf {
		s.Put(v.Zone, v.Service.ID, &conf[i])
	}

	for _, z := range []string{"alpha", "bravo", "charlie"} {
		for _, id := range s.Keys(z) {
			v, _ := s.Get(z, id).(*Config)
			if v == nil {
				fmt.Println("ERR", z, id, "not found")
				continue
			}
			fmt.Println(z, id, v.Name)
		}
	}

}
