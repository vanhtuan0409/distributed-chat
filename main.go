package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/go-redis/redis/v8"
)

var (
	port      int
	redisAddr string
)

func main() {
	flag.IntVar(&port, "port", 5656, "TCP binding port")
	flag.StringVar(&redisAddr, "redis", "localhost:6379", "Address of redis for pub/sub")
	flag.Parse()

	bindingAddr := fmt.Sprintf(":%d", port)
	l, err := net.Listen("tcp", bindingAddr)
	if err != nil {
		panic(err)
	}
	log.Printf("Server listened on %s", bindingAddr)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Accept error. ERR: %v", err)
			continue
		}
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	// each incomming connection
	// will create an equivalent connection to redis
	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		PoolSize:     1,
		MinIdleConns: 1,
	})
	defer rdb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		subscription := rdb.Subscribe(ctx, "chat")
		for {
			msg, err := subscription.ReceiveMessage(ctx)
			if err == redis.ErrClosed {
				break
			}
			if err != nil {
				log.Printf("Cannot recevied pub/sub message. ERR: %v", err)
				continue
			}
			fmt.Fprintf(conn, "%s\n", msg.Payload)
		}
	}()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg := scanner.Text()
		rdb.Publish(ctx, "chat", msg)
	}
}
