package messages

import (
	"github.com/anthdm/hollywood/actor"
	"github.com/shopspring/decimal"
	"time"
)

type Payment struct {
	CID         string  `json:"correlationId"`
	Amount      float64 `json:"amount"`
	RequestedAt string  `json:"requestedAt"`
}

type PushPayment struct {
	Payment     Payment
	ProcessedBy string
}

type PurgePayments struct{}

type SummarizePayments struct {
	From *time.Time
	To   *time.Time
}

type SummarizedProcessor struct {
	TotalRequests int64           `json:"totalRequests"`
	TotalAmount   decimal.Decimal `json:"totalAmount"`
}
type SummarizedPayments struct {
	Default  SummarizedProcessor `json:"default"`
	Fallback SummarizedProcessor `json:"fallback"`
}

type ProcessPayment struct {
	Payment Payment
	Tries   int
}

type ScheduleRetry struct {
	Sender  *actor.PID
	Payment Payment
	Tries   int
}

type Retry struct {
}
type PaymentProcessorChanged struct {
	Processor string
}
