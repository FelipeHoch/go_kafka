package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/felipehoch/go_kafka/dto"
)

type OrderService struct {
	TargetServiceURL string
	HttpClient       *http.Client
}

func NewOrderService(targetServiceURL string) (*OrderService, error) {
	if targetServiceURL == "" {
		return nil, errors.New("targetServiceURL is required")
	}

	return &OrderService{TargetServiceURL: targetServiceURL, HttpClient: &http.Client{Timeout: 5 * time.Second}}, nil
}

func (s *OrderService) ProcessMessage(message *kafka.Message) error {
	var order dto.Order

	err := json.Unmarshal(message.Value, &order)

	if err != nil {
		return errors.New(fmt.Sprintf("error unmarshalling message - event %s", string(message.Value)))
	}

	if !order.IsValid() {
		return errors.New(fmt.Sprintf("invalid order - event %s", string(message.Value)))
	}

	log.Printf("Processing order: %v", order.ID)

	err = retry.Do(
		func() error {
			return s.sendToTargetService(order)
		},
		retry.Attempts(3),
		retry.Delay(500*time.Millisecond),
		retry.MaxDelay(3*time.Second),
		retry.DelayType(retry.BackOffDelay),
		retry.OnRetry(func(n uint, err error) {
			log.Printf("Try %d failed: %v. Retrying...", n+1, err)
		}),
	)

	if err != nil {
		return errors.New(fmt.Sprintf("error sending request after multiple attempts - order %s", order.ID))
	}

	log.Printf("Order %s processed", order.ID)

	return nil
}

func (s *OrderService) sendToTargetService(order dto.Order) error {
	json, err := json.Marshal(order)

	if err != nil {
		return retry.Unrecoverable(errors.New(fmt.Sprintf("error serializing order - order %s - %v", order.ID, err)))
	}

	request, err := http.NewRequest("PATCH", s.TargetServiceURL, bytes.NewBuffer(json))

	if err != nil {
		return retry.Unrecoverable(errors.New(fmt.Sprintf("error creating request - order %s - %v", order.ID, err)))
	}

	request.Header.Set("Content-Type", "application/json")

	response, err := s.HttpClient.Do(request)

	if err != nil {
		return err
	}

	defer response.Body.Close()

	log.Printf("Response from target service: Status %d\n", response.StatusCode)

	switch {
	case response.StatusCode >= 200 && response.StatusCode < 300:
		return nil
	case response.StatusCode == 400:
		return retry.Unrecoverable(errors.New(fmt.Sprintf("bad request - status %d - order %s", response.StatusCode, order.ID)))
	case response.StatusCode == 404:
		return retry.Unrecoverable(errors.New(fmt.Sprintf("not found - status %d - order %s", response.StatusCode, order.ID)))
	case response.StatusCode >= 500:
		return errors.New(fmt.Sprintf("server error - status %d - order %s", response.StatusCode, order.ID))
	default:
		return retry.Unrecoverable(errors.New(fmt.Sprintf("unexpected client error - status %d - order %s", response.StatusCode, order.ID)))
	}
}
