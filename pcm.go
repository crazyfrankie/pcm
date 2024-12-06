package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

var (
	messageQueue []string   // 存储消息的队列
	mu           sync.Mutex // 用于保护共享资源（messageQueue）的互斥锁
	cond         *sync.Cond // 用于同步操作的条件变量
)

func main() {
	// 创建一个与mu关联的条件变量
	cond = sync.NewCond(&mu)

	// 启动生产者监听
	go startListenProducer()
	time.Sleep(time.Second * 2)

	// 启动消费者监听
	go startListenConsumer()
	time.Sleep(time.Second * 2)

	// 启动生产者发送数据
	go producer()
	time.Sleep(time.Second * 2)

	// 启动消费者接收数据
	go consumer()

	// 让主线程一直等待（阻塞）
	select {}
}

// startListenProducer 监听生产者连接
func startListenProducer() {
	// 在9090端口启动TCP监听
	listen, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatal(err)
	}
	defer listen.Close() // 程序退出时关闭监听

	fmt.Println("producer connection!")

	// 接受生产者连接请求，并为每个连接启动一个处理函数
	for {
		conn, er := listen.Accept()
		if er != nil {
			log.Fatal(er)
		}

		// 每个连接都由handleProducer处理
		go handleProducer(conn)
	}
}

// startListenConsumer 监听消费者连接
func startListenConsumer() {
	// 在9091端口启动TCP监听
	listen, err := net.Listen("tcp", ":9091")
	if err != nil {
		log.Fatal(err)
	}
	defer listen.Close() // 程序退出时关闭监听

	fmt.Println("consumer connection!")

	// 接受消费者连接请求，并为每个连接启动一个处理函数
	for {
		conn, er := listen.Accept()
		if er != nil {
			log.Fatal(er)
		}

		// 每个连接都由handleConsumer处理
		go handleConsumer(conn)
	}
}

// handleProducer 处理来自生产者的连接
func handleProducer(conn net.Conn) {
	defer conn.Close() // 连接关闭时释放资源

	// 生产者发送3条消息
	for i := 1; i <= 3; i++ {
		buf := make([]byte, 1024)
		// 读取生产者发送的消息
		n, err := conn.Read(buf)
		if err != nil {
			log.Fatalf("failed receive message%d from producer: %s", i, err)
		}

		// 加锁，向共享队列中添加消息
		mu.Lock()
		messageQueue = append(messageQueue, string(buf[:n]))
		// 唤醒等待的消费者
		cond.Signal()
		// 解锁
		mu.Unlock()
	}

	fmt.Println("producer send 3 messages, closing connection.")
}

// handleConsumer 处理来自消费者的连接
func handleConsumer(conn net.Conn) {
	defer conn.Close() // 连接关闭时释放资源

	// 消费者需要接收3条消息
	for i := 0; i < 3; i++ {
		// 加锁，访问共享资源
		mu.Lock()

		// 如果队列为空，消费者等待
		for len(messageQueue) == 0 {
			// 等待队列中有数据时再继续执行
			cond.Wait()
		}

		// 取出队列中的消息
		message := messageQueue[0]
		// 更新队列，移除已消费的消息
		messageQueue = messageQueue[1:]

		// 将消息发送给消费者
		_, err := conn.Write([]byte(message))
		if err != nil {
			log.Fatalf("failed send message to consumer: %s", err)
		}

		// 解锁
		mu.Unlock()
	}
}

// producer 生产者函数，从标准输入读取数据并发送给生产者端
func producer() {
	// 连接到生产者监听的端口（9090）
	conn, err := net.Dial("tcp", "localhost:9090")
	if err != nil {
		log.Fatalf("failed connect to localhost:9090 : %s", err)
	}
	defer conn.Close() // 连接关闭时释放资源

	// 使用Scanner从标准输入读取数据
	scanner := bufio.NewScanner(os.Stdin)

	// 读取并发送3条消息
	for i := 0; i < 3; i++ {
		// 从标准输入读取每一条消息
		if scanner.Scan() {
			message := scanner.Text()

			// 将消息发送到生产者端
			_, err := conn.Write([]byte(message))
			if err != nil {
				log.Fatalf("failed send message%d to broker:%s", i, err)
			}
		} else {
			log.Fatalf("failed to read input: %s", scanner.Err())
		}
	}
}

// consumer 消费者函数，接收生产者发送的数据并输出
func consumer() {
	// 连接到消费者监听的端口（9091）
	conn, err := net.Dial("tcp", "localhost:9091")
	if err != nil {
		log.Fatalf("failed connect to localhost:9091 : %s", err)
	}
	defer conn.Close() // 连接关闭时释放资源

	// 消费者接收3条消息
	for i := 1; i <= 3; i++ {
		buf := make([]byte, 1024)
		// 从消费者端接收消息
		n, err := conn.Read(buf)
		if err != nil {
			log.Fatalf("failed received from broker: %s", err)
		}

		// 输出接收到的消息
		fmt.Printf("consumer received message%d : %s\n", i, string(buf[:n]))
	}

	fmt.Println("consumer receive 3 messages, closing connection.")
}
