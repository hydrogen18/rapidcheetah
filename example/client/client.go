package main

import "os"
import "net"
import "log"
import "encoding/binary"
import "bufio"
import "io"
import "hash/adler32"
import "bytes"

func main() {
	destination := os.Args[1]
	name := os.Args[2]

	conn, err := net.Dial("tcp", destination)
	if err != nil {
		log.Panicf("Failed dialing %v:%v", destination, err)
	}
	log.Printf("Connected to %s", conn.RemoteAddr())

	go func() {
		var counter int
		var msg [256]byte
		for {
			var length uint8
			err := binary.Read(conn, binary.BigEndian, &length)
			if err != nil {
				log.Panicf("Failed reading:	%v", err)
			}

			var cksum uint32
			err = binary.Read(conn, binary.BigEndian, &cksum)
			if err != nil {
				log.Panicf("Failed reading:%v", err)
			}
			length -= 4

			_, err = conn.Read(msg[:length])
			if err != nil {
				log.Panicf("Failed reading:%v", err)
			}

			calc := adler32.Checksum(msg[:length])
			if cksum != calc {
				log.Panicf("Failed checksum %X != %X", cksum, calc)
			}

			log.Printf("%d:%d:%s", counter, length, string(msg[:length]))
			counter++

		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	maxLen := 255 - 1 - len(name) - 4

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) > maxLen {
			line = line[0:maxLen]
		}

		totalLen := byte(len(line) + 1 + len(name) + 4)
		err := binary.Write(conn, binary.BigEndian, &totalLen)
		if err != nil {
			log.Panicf("%v", err)
		}

		cksum := adler32.New()
		io.WriteString(cksum, name)
		io.WriteString(cksum, ":")
		io.Copy(cksum, bytes.NewReader(line))
		err = binary.Write(conn, binary.BigEndian, cksum.Sum32())
		if err != nil {
			log.Panicf("%v", err)
		}

		_, err = io.WriteString(conn, name)
		if err != nil {
			log.Panicf("%v", err)
		}

		_, err = io.WriteString(conn, ":")
		if err != nil {
			log.Panicf("%v", err)
		}

		_, err = conn.Write(line)
		if err != nil {
			log.Panicf("%v", err)
		}

	}
}
