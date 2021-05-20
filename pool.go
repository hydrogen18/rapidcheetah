package rapidcheetah

import "sync"
import "sync/atomic"

type ReferenceCountable interface {
	Decr()
	Incr()
}

type ReleasingReferenceCountFactory func() ReleasingReferenceCount

type ReferenceCountedPool struct {
	wrap      *sync.Pool
	returned  uint32
	allocated uint32
}

func NewReferenceCountedPool(New func(factory ReleasingReferenceCountFactory) ReferenceCountable) *ReferenceCountedPool {
	retval := &ReferenceCountedPool{}
	retval.wrap = &sync.Pool{}

	//Wrapped the passed in function in a closure
	retval.wrap.New = func() interface{} {
		n := New(retval.newReferenceCount)
		atomic.AddUint32(&retval.allocated, 1)
		return n
	}

	return retval
}

func (rcp *ReferenceCountedPool) newReferenceCount() ReleasingReferenceCount {
	return ReleasingReferenceCount{
		destination: rcp.wrap,
		release:     &rcp.returned,
	}
}

func (rcp *ReferenceCountedPool) Get() ReferenceCountable {
	rrc := rcp.wrap.Get().(ReferenceCountable)

	rrc.Incr()
	return rrc
}

func (rcp *ReferenceCountedPool) Returned() uint32 {
	return atomic.LoadUint32(&rcp.returned)
}

func (rcp *ReferenceCountedPool) Allocated() uint32 {
	return atomic.LoadUint32(&rcp.allocated)
}

type ReleasingReferenceCount struct {
	count       uint32
	destination *sync.Pool
	release     *uint32
	V           interface{}
}

func (rrc *ReleasingReferenceCount) Incr() {
	atomic.AddUint32(&rrc.count, 1)
}

func (rrc *ReleasingReferenceCount) Decr() {
	if atomic.AddUint32(&rrc.count, ^uint32(0)) == 0 {
		atomic.AddUint32(rrc.release, 1)
		rrc.destination.Put(rrc.V)
	}
}
