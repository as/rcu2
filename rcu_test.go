package rcu2

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"
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

func TestKeys(t *testing.T) {
	c := New()
	slist := []string{}
	flist := []string{}
	for i := range testbox {
		v := testbox[i]
		flist = append(flist, v.First)
		slist = append(slist, v.Second.ID)
		c.Put(v.First, v.Second.ID, &v)
	}
	keys := c.Keys()
	for i, have := range keys {
		want := flist[0]
		if have != want {
			t.Fatalf("first: %d: bad name: have %q, want %q", i, have, want)
		}
		sub := c.Keys(have)
		flist = flist[len(sub):]
		for j, have := range sub {
			want := slist[0]
			slist = slist[1:]
			if have != want {
				t.Fatalf("second: %d,%d: bad name: have %q, want %q", i, j, have, want)
			}
		}
	}
}

func hammer0(c *Store, stop <-chan bool, fn func(int)) {
	for i := 0; ; i++ {
		if i == len(testbox) {
			i = 0
		}
		select {
		case <-stop:
			return
		default:
		}
		fn(i)
		time.Sleep(time.Duration(rand.Int63n(int64(time.Second))))
	}
}
func hammer(c *Store, stop <-chan bool) {
	hammer0(c, stop, func(i int) {
		v := &testbox[i]
		c.Put(v.First, v.Second.ID, v)
	})
}
func hammer2(c *Store, stop <-chan bool) {
	hammer0(c, stop, func(i int) {
		v := &testbox[i]
		c.Del(v.First, v.Second.ID)
	})
}
func hammer3(c *Store, stop <-chan bool) {
	hammer0(c, stop, func(i int) {
		c.Get(fmt.Sprint(i), fmt.Sprint(i))
	})
}

func TestStoreRandom(t *testing.T) {
	c := New()
	done := make(chan bool)
	defer close(done)
	go hammer(c, done)
	go hammer(c, done)
	go hammer2(c, done)
	go hammer2(c, done)
	go hammer3(c, done)
	go hammer3(c, done)
	go hammer3(c, done)
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 5000; i++ {
		k0, k1 := fmt.Sprint(i, "f", "	", rand.Int()), fmt.Sprint("s", i, "	", rand.Int())
		v := Box{}
		v.First = k0
		v.Second.ID = k1
		const (
			max = 100
		)
		retry := 0
		for !c.Put(k0, k1, &v) && retry != max {
			retry++
		}
		if retry == max {
			t.Fatalf("put: failed to put %q, %q after %d attempts", k0, k1, retry)
		}
		if retry != 0 {
			t.Logf("put: contention: %q, %q (%d attempts) ", k0, k1, retry)
		}
		v0 := c.Get(k0, k1)
		if v0 == nil {
			t.Log("get", k0, k1, errNotExist)
			for _, nm := range c.Dir.Keys() {
				t.Log(nm)
			}
			t.FailNow()
		}
		if !reflect.DeepEqual(v0.(*Box), &v) {
			t.Fatalf("get: %q, %q: not deeply equal:\n\thave %#v\n\twant%#v", k0, k1, v0, v)
			break
		}
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
		v0 := c.Get(key0, key1)
		if v0 == nil {
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
				if v0 := c.Get(v.First, v.Second.ID); v0 == nil {
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
							if v0 := c.Get(v.First, v.Second.ID); v0 == nil {
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
							if v0 := c.Get(v.First, v.Second.ID); v0 == nil {
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
							if v0 := c.Get(v.First, v.Second.ID); v0 == nil {
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
