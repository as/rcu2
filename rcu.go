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
	c.storeFile(&File{Map: map[string]int{}})
	return c
}

// Get returns the path's key value. Ok is false if it
// does not exist.
func (s *Store) Get(path, key string) (v Value, ok bool) {
	x, ok := s.Dir.Get(path)
	if !ok {
		return x, false
	}
	return x.(*Dir).Get(key)
}

// Put associates the value with the path's key. It returns
// false if a concurrent modification prevented the call from
// completing the Put.
//
// TODO(as): Algorithm complexity
func (s *Store) Put(path, key string, val Value) bool {
	dmap := s.loaddir().loadFile()

	sbox, _ := dmap.Get(path).(*Dir)
	if sbox == nil {
		sbox = &Dir{}
		sbox.storeFile(&File{Map: map[string]int{}})
		smap := sbox.loadFile()
		sbox.Swap(smap, key, val)
		return s.Dir.Swap(dmap, path, sbox)
	}

	smap := sbox.loadFile()
	return sbox.Swap(smap, key, val)
}

// Del deletes the path's key value. It returns true if the
// value does not exist after the call completed (this could
// mean the value never existed). 
//
// It returns false if and only if a concurrent modification 
// prevented the operation from ascertaining that the key
// no longer exists.
func (s *Store) Del(path, key string) (Value, bool) {
	d, ok := s.loaddir().Get(path)
	if !ok {
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
// Swap returns true
func (d *Dir) Swap(f *File, key string, val Value) bool {
	i, ok := f.Map[key]
	if !ok {
		// new key val, so copy and update
		return d.casFile(f, f.Add(key, val))
	}
	// old key, update w/ val, storing it in place
	ptr := unsafe.Pointer(&f.Value[i])
	atomic.StorePointer(&ptr, unsafe.Pointer(&val))
	return true
}

// Get returns the value for the key. Ok is true if the key exists.
func (d *Dir) Get(key string) (v Value, ok bool) {
	v = d.loadFile().Get(key)
	return v, v != nil
}

// Del deletes the named key. It returns non-nil v if the value existed
// and was successfully deleted. It returns !ok if a concurrent write
// occured.
//
// Most callers should just check the value of ok
func (d *Dir) Del(key string) (v Value, ok bool) {
	f := d.loadFile()
	i, ok := f.Map[key]
	if !ok {
		return nil, true
	}
	if !d.casFile(f, f.clone(i)) {
		return nil, false
	}
	return &f.Value[i], true
}

func (d *Dir) loadFile() *File {
	return (*File)(atomic.LoadPointer(&d.File))
}
func (d *Dir) storeFile(b *File) {
	atomic.StorePointer(&d.File, unsafe.Pointer(b))
}
func (d *Dir) casFile(old, new *File) bool {
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

func (f *File) clone(exclude ...int) *File {
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
