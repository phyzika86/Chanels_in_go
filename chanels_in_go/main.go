package main

import (
	"fmt"

	"time"
)

type Order struct {
	ID       int
	Customer string
}

func main() {
	// Критерий 1: Используем небуферизированный канал для передачи заказов
	ordersCh := make(chan Order)

	// Канал для синхронизации: воркеры сообщат в него о завершении своей работы
	doneCh := make(chan bool)

	numWorkers := 3

	// Запускаем 3 горутины-обработчика
	for i := 1; i <= numWorkers; i++ {
		// Передаем порядковый номер, канал с заказами и канал для сигнала готовности
		go worker(i, ordersCh, doneCh)
	}

	// Запускаем генератор заказов в отдельной горутине.
	// Так как канал небуферизированный, генератор будет отправлять заказ только
	// тогда, когда освободится хотя бы один воркер и будет готов его принять.
	go generateOrders(ordersCh)

	// Ждем завершения всех воркеров без использования sync.WaitGroup.
	// Мы знаем, что запустили 3 воркера, поэтому ровно 3 раза читаем из doneCh.
	for i := 1; i <= numWorkers; i++ {
		<-doneCh
	}

	fmt.Println("🚀 Все заказы обработаны, программа корректно завершена!")
}

func generateOrders(ch chan<- Order) {
	customers := []string{"Алексей", "Мария", "Иван", "Елена", "Дмитрий"}

	for i := 1; i <= 10; i++ {
		// Выбираем имя по кругу с помощью остатка от деления
		customer := customers[i%len(customers)]

		ch <- Order{ID: i, Customer: customer}
	}

	// Закрываем канал, чтобы читатель не заблокировался в бесконечном ожидании
	close(ch)
}

// Функция-обработчик (Воркер)
func worker(id int, ordersCh <-chan Order, doneCh chan<- bool) {
	// Критерий 5: Цикл range автоматически и корректно завершится при закрытии канала
	for order := range ordersCh {
		fmt.Printf("[Воркер %d] Взял в работу заказ #%d для %s\n", id, order.ID, order.Customer)

		// Имитация обработки заказа
		time.Sleep(500 * time.Millisecond)

		fmt.Printf("[Воркер %d] ✅ Успешно выполнил заказ #%d\n", id, order.ID)
	}

	fmt.Printf("[Воркер %d] 🛑 Заказы закончились, воркер завершает работу.\n", id)

	// Сигнализируем функции main, что этот воркер закончил свою работу
	doneCh <- true
}
