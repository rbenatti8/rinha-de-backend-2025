package actors

type checker interface {
	GetPaymentProcessor() (string, error)
	HasHealthyProcessors() bool
}
