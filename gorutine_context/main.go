package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// OrderResult содержит информацию об итогах обработки заказа.
type OrderResult struct {
	OrderID int
	Err     error
}

func main() {
	// 1. Создаем контекст для всей системы с таймаутом 10 секунд.
	// Если система работает дольше 10 секунд, все дочерние контексты автоматически отменятся.
	sysCtx, sysCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer sysCancel()

	// Настраиваем генератор случайных чисел для симуляции времени обработки.

	numOrders := 5
	resultsChan := make(chan OrderResult, numOrders)
	var wg sync.WaitGroup

	fmt.Println("Запуск системы обработки заказов")

	// 2. Запускаем обработку каждого заказа в отдельной горутине.
	for i := 1; i <= numOrders; i++ {
		wg.Add(1)
		go processOrder(sysCtx, i, &wg, resultsChan)
	}

	// Горутина для закрытия канала после того, как все обработчики завершат работу.
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// 3. Сбор и вывод результатов работы из канала.
	for res := range resultsChan {
		if res.Err != nil {
			fmt.Printf("Заказ №%d завершился с ошибкой: %v\n", res.OrderID, res.Err)
		} else {
			fmt.Printf("Заказ №%d успешно учтен в базе данных\n", res.OrderID)
		}
	}

	fmt.Println("Работа системы завершена")
}

// processOrder имитирует обработку одного конкретного заказа.
func processOrder(parentCtx context.Context, orderID int, wg *sync.WaitGroup, results chan<- OrderResult) {
	defer wg.Done()

	// 4. Для каждой горутины создаем свой контекст с таймаутом 3 секунды на базе родительского.
	orderCtx, orderCancel := context.WithTimeout(parentCtx, 3*time.Second)
	defer orderCancel()

	// Сообщение о начале обработки.
	fmt.Printf("[Заказ №%d] Начало обработки...\n", orderID)

	// Имитируем случайное время обработки заказа от 1 до 5 секунд.
	// Если выпадет 4 или 5 секунд — сработает таймаут заказа (3 сек).
	simulatedDuration := time.Duration(rand.Intn(5)+1) * time.Second
	processTimer := time.NewTimer(simulatedDuration)
	defer processTimer.Stop()

	// 5. Обработка отмены по контексту или успешного завершения через select.
	select {
	case <-orderCtx.Done():
		// Контекст может быть отменен по двум причинам:
		// - Вышел локальный таймаут заказа (3 секунды) -> context.DeadlineExceeded
		// - Вышел общий таймаут всей системы (10 секунд) -> parentCtx.Err()
		err := orderCtx.Err()
		fmt.Printf("[Заказ №%d] ОБРАБОТКА ОТМЕНЕНА: %v\n", orderID, err)
		results <- OrderResult{OrderID: orderID, Err: err}

	case <-processTimer.C:
		// Имитация успешного завершения операции.
		fmt.Printf("[Заказ №%d] Успешно завершен за %v!\n", orderID, simulatedDuration)
		results <- OrderResult{OrderID: orderID, Err: nil}
	}
}
