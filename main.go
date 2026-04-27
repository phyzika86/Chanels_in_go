package main

import (
	"fmt"

	"time"
)

var counter int

func increment() {

	counter++

}

func example_incr() {
	for i := 0; i < 1000; i++ {

		go increment()

	}

	time.Sleep(1 * time.Second)

	fmt.Println("Final counter value:", counter)
}

func simple_gorutine() {

	for i := 0; i < 5; i++ {

		go func() {

			// Искусственная задержка для захвата последнего значения i

			time.Sleep(10 * time.Millisecond)

			fmt.Println(i) // горутины увидят i=5

		}()

	}

	// Даем горутинам время выполниться, но не исправляем саму проблему

	time.Sleep(1000 * time.Millisecond)
}

func chanale() {
	ch := make(chan int)

	go func() {

		fmt.Println("Отправляю значение...")

		ch <- 42

		fmt.Println("Значение отправлено.")

	}()

	time.Sleep(time.Second)

	val := <-ch

	fmt.Println("Получено значение:", val)
}

func main() {
	go chanale()
	time.Sleep(time.Second * 10)
}
