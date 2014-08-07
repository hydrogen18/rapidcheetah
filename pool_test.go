package rapidcheetah

import "testing"
import "sync"
import "os"
import "strconv"

type testObject struct {
	count int
	data  [32]byte
}

type testObjectWithReferenceCount struct {
	testObject
	ReleasingReferenceCount
}

func testObjectFactory(f ReleasingReferenceCountFactory) ReferenceCountable {
	r := &testObjectWithReferenceCount{}
	r.ReleasingReferenceCount = f()
	r.V = r
	return r
}

const ACQUIRE_N = 32
const POOL_MAX_SIZE = 65535

func getAcquireN() int {

	config := os.Getenv("acquiren")
	if len(config) != 0 {
		n, err := strconv.Atoi(config)
		if err == nil {
			return n
		}
	}

	return ACQUIRE_N
}

func getWorkers() int {
	return 4
}

func BenchmarkWrappedPool(b *testing.B) {
	workerCount := getWorkers()
	acquireN := getAcquireN()
	var testPool = NewReferenceCountedPool(testObjectFactory)

	wg := &sync.WaitGroup{}
	worker := func(wg *sync.WaitGroup) {
		var objs []ReferenceCountable = make([]ReferenceCountable, acquireN)

		for i := 0; i != b.N; i++ {
			for j := 0; j != acquireN; j++ {
				objs[j] = testPool.Get()
			}

			for j := 0; j != acquireN; j++ {
				objs[j].Decr()
			}
		}
		wg.Done()
	}

	for i := 0; i != workerCount; i++ {
		wg.Add(1)
		go worker(wg)
	}
	wg.Wait()
}

func BenchmarkSyncPool(b *testing.B) {
	testPool := &sync.Pool{}
	testPool.New = func() interface{} {
		return new(testObject)
	}

	wg := &sync.WaitGroup{}
	acquireN := getAcquireN()

	worker := func(wg *sync.WaitGroup) {
		var objs []interface{} = make([]interface{}, acquireN)

		for i := 0; i != b.N; i++ {

			for j := 0; j != acquireN; j++ {
				objs[j] = testPool.Get()
			}

			for j := 0; j != acquireN; j++ {
				testPool.Put(objs[j])
			}

		}
		wg.Done()
	}
	workerCount := getWorkers()
	for i := 0; i != workerCount; i++ {
		wg.Add(1)
		go worker(wg)
	}
	wg.Wait()
}
