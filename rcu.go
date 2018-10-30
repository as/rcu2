package rcu2

import (
	"sync/atomic"
	"unsafe"
)

type Value interface{}
type Store struct {
	Dir
}

// New returns an initialized Store
func New() *Store {
	c := &Store{Dir: Dir{}}
	// this is ok; nobody can access c.Dir yet
	c.Dir.store(&File{Map: map[string]int{}})
	return c
}

func (s *Store) Names() []string {
	return nil
}

// Get returns the path's key value. Ok is false if it
// does not exist.
func (s *Store) Get(path, key string) (v Value) {
	v0, _ := s.Dir.Get(path).(*Dir)
	return v0.Get(key)
}

func newDir(key string, val Value) *Dir {
	d := &Dir{}
	d.store(&File{Map: map[string]int{}})
	d.Swap(d.checkout(), key, val)
	return d
}

// Put associates the value with the path's key. It returns
// false if a concurrent modification prevented the call from
// completing the Put.
//
// TODO(as): Algorithm complexity
func (s *Store) Put(path, key string, val Value) bool {
	d0 := s.loaddir()
	f0 := d0.checkout()

	pdir, _ := f0.Get(path).(*Dir)
	if pdir == nil {
		return d0.Swap(f0, path, newDir(key, val))
	}

	return pdir.Swap(pdir.checkout(), key, val)
}

// Del deletes the path's key value. It returns true if the
// value does not exist after the call completed (this could
// mean the value never existed).
//
// It returns false if and only if a concurrent modification
// prevented the operation from ascertaining that the key
// no longer exists.
func (s *Store) Del(path, key string) (Value, bool) {
	d := s.loaddir().Get(path)
	if d == nil {
		return nil, true
	}
	return d.(*Dir).Del(key)
}

func (c *Store) loaddir() *Dir {
	tmp := unsafe.Pointer(&c.Dir)
	return (*Dir)(atomic.LoadPointer(&tmp))
}

type Dir struct {
	File unsafe.Pointer // *File
}

// Swap stores the key and value in f. If key already exists
// in f, the swap operation is O(1), otherwise, it's O(len(f))
// due to RCU.
//
// Swap returns false if a concurrent modification occured
// during the call.
func (d *Dir) Swap(f *File, key string, val Value) bool {
	i, ok := f.Map[key]
	if !ok {
		// new key val, so copy and update
		return d.checkin(f, f.Add(key, val))
	}
	// old key, update w/ val, storing it in place
	ptr := unsafe.Pointer(&f.Value[i])
	atomic.StorePointer(&ptr, unsafe.Pointer(&val))
	return true
}

// Key returns a list of key names
func (d *Dir) Keys(names ...string) []string {
	if len(names) == 0 {
		return d.checkout().Name
	}

	dir := d.checkout()
	if len(names) == 1 {
		dir1, _ := dir.Get(names[0]).(*Dir)
		if dir1 == nil {
			return nil
		}
		return dir1.Keys()
	}

	s := make([]string, 0, 25*len(names))
	for _, n := range names {
		dir1, _ := dir.Get(n).(*Dir)
		if dir1 == nil {
			continue
		}
		s = append(s, dir1.Keys()...)
	}
	return s
}

// Get returns the value for the key. Ok is true if the key exists.
func (d *Dir) Get(key string) (v Value) {
	return d.checkout().Get(key)
}

// Del deletes the named key. It returns non-nil v if the value existed
// and was successfully deleted. It returns !ok if a concurrent write
// occured.
//
// Most callers should just check the value of ok
func (d *Dir) Del(key string) (v Value, ok bool) {
	f := d.checkout()
	i, ok := f.Map[key]
	if !ok {
		return nil, true
	}
	if !d.checkin(f, f.clone(i)) {
		return nil, false
	}
	return &f.Value[i], true
}

func (d *Dir) checkout() *File {
	if d == nil {
		return nil
	}
	return (*File)(atomic.LoadPointer(&d.File))
}
func (d *Dir) store(b *File) {
	atomic.StorePointer(&d.File, unsafe.Pointer(b))
}
func (d *Dir) checkin(old, new *File) bool {
	return atomic.CompareAndSwapPointer(
		&d.File,
		unsafe.Pointer(old),
		unsafe.Pointer(new),
	)
}

type File struct {
	Map   map[string]int // maps names to index in Value
	Value []Value
	Name  []string // Names in []Value
}

func (f *File) Get(key string) Value {
	if f == nil {
		return nil
	}
	i, ok := f.Map[key]
	if !ok {
		return nil
	}
	ptr := unsafe.Pointer(&f.Value[i])
	return *((*Value)(atomic.LoadPointer(&ptr)))
}

func (f *File) Del(key string) *File {
	i, ok := f.Map[key]
	if !ok {
		return f
	}
	return f.clone(i)
}

func (f *File) Add(key string, v Value) *File {
	f0 := f.clone()
	f0.Name = append(f0.Name, key)
	f0.Map[key] = len(f0.Value)
	f0.Value = append(f0.Value, v)
	return f0
}

func (f File) clone(exclude ...int) *File {
	delta := -len(exclude)
	if delta == 0 {
		// we aren't excluding, so we're adding, make room for one add
		delta = 1
	}
	conf := make([]Value, 0, len(f.Value)+delta)
	name := make([]string, 0, len(f.Name)+delta)
	mp := make(map[string]int, len(f.Map)+delta)

	for i := range f.Value {
		if len(exclude) > 0 && exclude[0] == i {
			exclude = exclude[1:]
			continue
		}
		mp[f.Name[i]] = len(name)
		conf = append(conf, f.Value[i])
		name = append(name, f.Name[i])
	}

	return &File{
		Value: conf,
		Name:  name,
		Map:   mp,
	}
}
