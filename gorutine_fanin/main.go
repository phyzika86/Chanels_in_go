package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Metric struct {
	Source string    // название источника: строго “CPU”, “Memory” или "Network"
	Value  float64   // значение метрики (случайное число в определенном диапазоне)
	Time   time.Time // время создания метрики (time.Now())
}

func cpuMetrics() <-chan Metric {
	/*
		Создаем канал с метриками ЦПУ
	*/
	ch := make(chan Metric)
	go func() {
		defer close(ch)
		for i := 0; i < 5; i++ {
			time.Sleep(800 * time.Millisecond)
			// Каждые 800 милисикунд складываем в канал метрику. Положим 5 метрик.
			ch <- Metric{
				Source: "CPU",
				Value:  rand.Float64() * 100,
				Time:   time.Now(),
			}
		}
	}()
	// Запускаем горутину и сразу возвращаем канал на чтение получателю
	return ch
}

func memoryMetrics() <-chan Metric {
	ch := make(chan Metric)
	go func() {
		defer close(ch)
		for i := 0; i < 5; i++ {
			time.Sleep(1200 * time.Millisecond)
			ch <- Metric{
				Source: "Memory",
				Value:  rand.Float64() * 16384,
				Time:   time.Now(),
			}
		}
	}()
	return ch
}

func networkMetrics() <-chan Metric {
	ch := make(chan Metric)
	go func() {
		defer close(ch)
		for i := 0; i < 5; i++ {
			time.Sleep(1500 * time.Millisecond)
			ch <- Metric{
				Source: "Network",
				Value:  rand.Float64() * 1000,
				Time:   time.Now(),
			}
		}
	}()
	return ch
}

func fanIn(channels ...<-chan Metric) <-chan Metric {
	// Агрегирующий канал
	out := make(chan Metric)

	var wg sync.WaitGroup

	// Запускаем горутину, которая читает из каждого канала
	for _, ch := range channels {
		wg.Add(1)
		go func(c <-chan Metric) {
			defer wg.Done()
			for m := range c {
				out <- m
			}
		}(ch)
	}

	// Ждем, пока все горутины, пишущие в общий канал не выполнятся
	go func() {
		wg.Wait()
		close(out)
	}()

	// Возвращаем канал на чтение
	return out
}

func main() {
	rand.Seed(time.Now().UnixNano())

	fmt.Println("Система мониторинга запущена...")

	// Создание трех каналов
	cpuCh := cpuMetrics()
	memCh := memoryMetrics()
	netCh := networkMetrics()

	// Передача каналов в fanIn и получение объединенного канала
	mergedCh := fanIn(cpuCh, memCh, netCh)

	// Чтение и вывод метрик в точном формате
	for metric := range mergedCh {
		fmt.Printf("Источник: %s, Значение: %.2f, Время: %v\n",
			metric.Source, metric.Value, metric.Time.Format("15:04:05"))
	}

	// Сообщение о завершении
	fmt.Println("Сбор метрик завершен.")
}
