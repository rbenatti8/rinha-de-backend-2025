package messages

import (
	"github.com/anthdm/hollywood/actor"
	"github.com/shopspring/decimal"
	"time"
)

type Payment struct {
	Amount      float64 `json:"amount"`
	CID         string  `json:"correlationId"`
	RequestedAt string  `json:"requestedAt"`
}

type PushPayment struct {
	Payment     Payment
	ProcessedAt time.Time
	ProcessedBy string
}

type PushToRedis struct{}

type PurgePayments struct{}

type SummarizePayments struct {
	From *time.Time
	To   *time.Time
}

type SummarizedProcessor struct {
	TotalAmount   decimal.Decimal `json:"totalAmount"`
	TotalRequests int64           `json:"totalRequests"`
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

type CheckIntegrity struct {
	Payment   Payment
	Processor string
}
