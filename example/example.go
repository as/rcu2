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

	fmt.Println("#")
	fmt.Println("# Keys are known in advance (query)")
	for _, z := range []string{"alpha", "bravo", "charlie"} {
		for _, id := range s.Keys(z) {
			if v, _ := s.Get(z, id).(*Config); v != nil{
				fmt.Println(id, v.Name)
			}
		}
	}
	fmt.Println("")

	fmt.Println("#")
	fmt.Println("# Keys are not known (range)")
	for _, z := range s.Keys() {
		for _, id := range s.Keys(z) {
			if v, _ := s.Get(z, id).(*Config); v != nil{
				fmt.Println(id, v.Name)
			}
		}
	}
	fmt.Println("")

}
