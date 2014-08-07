package main

import "github.com/hydrogen18/rapidcheetah"
import "net"
import "log"
import "sync/atomic"
import "encoding/binary"
import "time"
import "runtime"

type ChatLine struct {
	message [256]byte
	length  int
	rapidcheetah.ReleasingReferenceCount
}

func chatLineFactory(f rapidcheetah.ReleasingReferenceCountFactory) rapidcheetah.ReferenceCountable {
	cl := &ChatLine{}
	cl.ReleasingReferenceCount = f()
	cl.V = cl
	return cl
}

type worker struct {
	pool     *rapidcheetah.ReferenceCountedPool
	incoming chan *ChatLine
	outgoing chan<- *ChatLine
	alive    uint32
	conn     net.Conn
}

func (w *worker) IsAlive() bool {
	return 0 != atomic.LoadUint32(&w.alive)
}

func (w *worker) Send() {
	defer atomic.AddUint32(&w.alive, ^uint32(0))
	defer w.conn.Close()
	for {
		var msg *ChatLine
		select {
		case msg = <-w.incoming:
		}

		_, err := w.conn.Write(msg.message[:msg.length])
		msg.Decr()
		msg = nil
		if err != nil {
			log.Printf("Connection %s send failure %v", w.conn.RemoteAddr(), err)
			return
		}

	}
}

func (w *worker) Recv() {
	defer atomic.AddUint32(&w.alive, ^uint32(0))
	defer w.conn.Close()

	for {
		var length byte
		err := binary.Read(w.conn, binary.BigEndian, &length)

		if err != nil {
			log.Printf("Connection %s recv failure %v", w.conn.RemoteAddr(), err)
			return
		}

		//Get an instance from the pool, reference count is already one
		msg := w.pool.Get().(*ChatLine)
		msg.length = int(length) + 1
		msg.message[0] = length
		amountRead := 0
		amountToRead := int(length)

		for amountToRead != amountRead {
			n, err := w.conn.Read(msg.message[1+amountRead : 1+int(length)])
			if err != nil {
				log.Printf("Connection %s recv failure %v", w.conn.RemoteAddr(), err)
				//Message not used, decrement reference count
				msg.Decr()
				return
			}
			amountRead += n

		}

		w.outgoing <- msg
	}

}

func distributor(newWorkers <-chan *worker, messages <-chan *ChatLine, pool *rapidcheetah.ReferenceCountedPool) {
	workers := make([]*worker, 0, 8)

	//Check for closed workers every 10 seconds
	reaper := time.NewTicker(10 * time.Second)

	memstats := &runtime.MemStats{}

	for {
		select {
		case _ = <-reaper.C:
			//Rebuild the 'workers' slice
			replacement := make([]*worker, 0, len(workers))
			var reaped int
			for _, w := range workers {
				//If the worker is still running, keep it
				if w.IsAlive() {
					replacement = append(replacement, w)
					continue
				}

				//Close the channel
				close(w.incoming)
				//Decrement the reference count any remaining messages so they
				//can be reclaimed
				for chatLine := range w.incoming {
					chatLine.Decr()
				}
				reaped += 1
			}
			//Replace slice
			workers = replacement
			log.Printf("Reaped %d workers", reaped)

			runtime.ReadMemStats(memstats)
			log.Printf("TotalAlloc:%d bytes", memstats.TotalAlloc)
			log.Printf("HeapObjects:%d objects", memstats.HeapObjects)
			log.Printf("Returned:%d objects", pool.Returned())
			log.Printf("Allocated:%d objects", pool.Allocated())
		case newWorker := <-newWorkers:
			workers = append(workers, newWorker)
		case msg := <-messages:
			//Reference count is already one here
			for _, w := range workers {
				//Increment the reference count before passing the object
				//to each worker
				msg.Incr()
				select {
				case w.incoming <- msg:
				default:
					//Failed passing the object to the worker. Decrement the
					//reference count
					msg.Decr()
				}
			}
			//Decrement reference count here
			msg.Decr()

		}
	}

}

func main() {

	listener, err := net.Listen("tcp", ":52000")

	pool := rapidcheetah.NewReferenceCountedPool(chatLineFactory)

	if err != nil {
		log.Panicf("Failed opening listener:%v", err)
	}

	newWorkers := make(chan *worker, 1)
	messages := make(chan *ChatLine, 64)
	go distributor(newWorkers, messages, pool)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Panicf("Failed accepting:%v", err)
		}

		w := &worker{}
		w.incoming = make(chan *ChatLine, 1)
		w.outgoing = messages
		w.conn = conn
		w.alive = 2
		w.pool = pool
		go w.Send()
		go w.Recv()
		newWorkers <- w
	}

}
