package rcu2

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"unsafe"
)

var errNotExist = errors.New("key not found")
var errConcurrentWrite = errors.New("concurrent update")

type Box struct {
	Second Second
	First  string
	Blah   int
}
type Second struct {
	ID string
}

var testbox = [...]Box{
	{Second: Second{ID: "a1"}, First: "a"},
	{Second: Second{ID: "a2"}, First: "a"},
	{Second: Second{ID: "a3"}, First: "a"},
	{Second: Second{ID: "b1"}, First: "b"},
	{Second: Second{ID: "b2"}, First: "b"},
	{Second: Second{ID: "b3"}, First: "b"},
	{Second: Second{ID: "c1"}, First: "c"},
	{Second: Second{ID: "c2"}, First: "c"},
	{Second: Second{ID: "c3"}, First: "c"},
}
var testboxd = [...]Box{
	{Second: Second{ID: "d1"}, First: "d"},
	{Second: Second{ID: "d2"}, First: "d"},
	{Second: Second{ID: "d3"}, First: "d"},
}

func hammer(c *Store, stop <-chan bool) {
	for i := 0; ; i++ {
		if i == len(testbox) {
			i = 0
			select {
			case <-stop:
				return
			default:
			}
		}
		v := &testbox[i]
		c.Put(v.First, v.Second.ID, v)
	}
}
func hammer2(c *Store, stop <-chan bool) {
	for i := 0; ; i++ {
		if i == len(testbox) {
			i = 0
			select {
			case <-stop:
				return
			default:
			}
		}
		v := &testbox[i]
		c.Del(v.First, v.Second.ID)
	}
}

func TestStoreConcurrent(t *testing.T) {
	c := New()
	done := make(chan bool)
	defer close(done)
	go hammer(c, done)
	go hammer(c, done)
	go hammer(c, done)

	for i := 0; i < 10000; i++ {
		v := testbox[i%len(testbox)]
		key0, key1 := v.First, v.Second.ID
		if !c.Put(key0, key1, &v) {
			// don't fail the test, this behavior is fine
			t.Log("put", i, errConcurrentWrite)
		}
		v0, ok := c.Get(key0, key1)
		if !ok {
			t.Fatal("get", i, errNotExist)
		}
		if !reflect.DeepEqual(v0.(*Box), &v) {
			t.Fatalf("get: %q, %q: not deeply equal:\n\thave %#v\n\twant%#v", key0, key1, v0, v)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	c := New()

	for i := range testbox {
		v := &testbox[i]
		c.Put(v.First, v.Second.ID, v)
	}

	b.Run("Serial", func(b *testing.B) {
		b.SetBytes(int64(unsafe.Sizeof(testbox)))
		for n := 0; n < b.N; n++ {
			for i := range testbox {
				v := &testbox[i]
				_, ok := c.Get(v.First, v.Second.ID)
				if !ok {
					b.Fatal("get", i, errNotExist)
				}
			}
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		for _, cpu := range []int{0, 1, 2, 4} {
			b.Run(fmt.Sprintf("%dWriters", cpu), func(b *testing.B) {
				done := make(chan bool)
				defer close(done)
				for x := 0; x < cpu; x++ {
					go hammer(c, done)
				}
				b.SetBytes(int64(unsafe.Sizeof(testbox)))

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						for i := range testbox {
							v := &testbox[i]
							_, ok := c.Get(v.First, v.Second.ID)
							if !ok {
								b.Fatal("get", i, errNotExist)
							}
						}
					}
				})

			})
		}
	})

	b.Run("2048Firsts25Seconkey0", func(b *testing.B) {
		for i := 0; i < 2048; i++ {
			for j := 0; j < 25; j++ {
				v := &Box{First: fmt.Sprint(i), Second: Second{ID: fmt.Sprint(j)}}
				c.Put(v.First, v.Second.ID, v)
			}
		}

		for _, cpu := range []int{0, 1, 2, 4} {
			b.Run(fmt.Sprintf("%dWriters", cpu), func(b *testing.B) {
				done := make(chan bool)
				defer close(done)
				for x := 0; x < cpu; x++ {
					go hammer(c, done)
				}
				b.SetBytes(int64(unsafe.Sizeof(testbox)))

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						for i := range testbox {
							v := &testbox[i]
							_, ok := c.Get(v.First, v.Second.ID)
							if !ok {
								b.Fatal("get", i, errNotExist)
							}
						}
					}
				})

			})
		}
	})

	for i := range testboxd {
		v := &testboxd[i]
		c.Put(v.First, v.Second.ID, v)
	}

	b.Run("ParallelUncontestedFirst", func(b *testing.B) {
		for _, cpu := range []int{0, 1, 2, 4} {
			b.Run(fmt.Sprintf("%dWriters", cpu), func(b *testing.B) {
				done := make(chan bool)
				defer close(done)
				for x := 0; x < cpu; x++ {
					go hammer(c, done)
				}
				b.SetBytes(int64(unsafe.Sizeof(testbox)))

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						for i := range testboxd {
							v := &testboxd[i]
							_, ok := c.Get(v.First, v.Second.ID)
							if !ok {
								b.Fatal("get", i, errNotExist)
							}
						}
					}
				})

			})
		}
	})

}

func BenchmarkPut(b *testing.B) {
	c := New()
	b.SetBytes(int64(unsafe.Sizeof(testbox)))

	b.Run("Serial", func(b *testing.B) {
		b.SetBytes(int64(unsafe.Sizeof(testbox)))
		for n := 0; n < b.N; n++ {
			for i, v := range testbox {
				if !c.Put(v.First, v.Second.ID, &v) {
					b.Fatal("put", i, errConcurrentWrite)
				}
			}
		}
	})

	b.Run("Parallel", func(b *testing.B) {

		b.SetBytes(int64(unsafe.Sizeof(testbox)))
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				for i, v := range testbox {
					if !c.Put(v.First, v.Second.ID, &v) {
						b.Fatal("put", i, errConcurrentWrite)
					}
				}
			}
		})
	})
}
