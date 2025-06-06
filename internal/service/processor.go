package service

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"bytes"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/felipehoch/go_kafka/dto"
	"github.com/avast/retry-go/v4"
)

type OrderService struct {
	TargetServiceURL string
}

func NewOrderService(targetServiceURL string) (*OrderService, error) {
	if targetServiceURL == "" {
		return nil, errors.New("targetServiceURL is required")
	}

	return &OrderService{TargetServiceURL: targetServiceURL}, nil
}

func (s *OrderService) ProcessMessage(message *kafka.Message) error {
	var order dto.Order

	err := json.Unmarshal(message.Value, &order)
	
	if err != nil {
		return errors.New(fmt.Sprintf("error unmarshalling message - event %s", message.Value))
	}

	if !order.IsAnValidOrder() {		
		return errors.New(fmt.Sprintf("invalid order - event %s", message.Value))
	}

	err = retry.Do(
		func() error {
			return s.sendToTargetService(order)
		},
		retry.Attempts(2),                    
		retry.Delay(1*time.Second),          
		retry.MaxDelay(5*time.Second),
		retry.DelayType(retry.BackOffDelay),
		retry.OnRetry(func(n uint, err error) {
			log.Printf("Tentativa %d falhou: %v. Tentando novamente...", n+1, err)
		}),
	)
	
	if err != nil {
		return errors.New("error sending request after multiple attempts")
	}
	
	log.Printf("Order %s billed", order.ID)

	return nil
}

func (s *OrderService) sendToTargetService(order dto.Order) error {
	json, err := json.Marshal(order)

	if err != nil {
		return retry.Unrecoverable(errors.New("error serializing order"))
	}

	request, err := http.NewRequest("PATCH", s.TargetServiceURL, bytes.NewBuffer(json))

	if err != nil {
		return retry.Unrecoverable(errors.New("error creating request"))
	}

	request.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout:  5 * time.Second,
	}

	response, err := client.Do(request)

	if err != nil {
		return err
	}

	defer response.Body.Close()

	log.Printf("Response from target service: Status %d\n", response.StatusCode)

	if response.StatusCode >= 300 && response.StatusCode < 500 {
		return retry.Unrecoverable(errors.New(fmt.Sprintf("unexpected client error - status %d", response.StatusCode)))
	}

	if response.StatusCode >= 500 {
		return errors.New(fmt.Sprintf("server error - status %d", response.StatusCode))
	}

	return nil
}