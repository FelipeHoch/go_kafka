package dto

import (
	"encoding/json"
	"strings"
)

type Order struct {
	ID      string  `json:"ordemDeVenda"`
	Status  string  `json:"etapaAtual"`
	TraceID *string `json:"traceId"`
}

func (o *Order) IsValid() bool {
	return o.ID != "" && o.Status != "" && strings.ToUpper(o.Status) == "FATURADO"
}

func (o *Order) ToJSON() ([]byte, error) {
	payload := map[string]string{
		"ordemDeVenda": o.ID,
		"etapaAtual":   o.Status,
	}

	return json.Marshal(payload)
}
