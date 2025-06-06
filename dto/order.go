package dto

import "strings"

type Order struct {
	ID string `json:"ordemDeVenda"`
	Status string `json:"etapaAtual"`
}

func (o *Order) IsAnValidOrder() bool {
	return o.ID != "" && o.Status != "" && strings.ToUpper(o.Status) == "FATURADO"
}