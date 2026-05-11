package common

type CustomerRequest struct {
	RequestID   string
	AccountID   int64
	FromAccountID int64
	ToAccountID int64
	AmountCents int64
}

type CustomerReply struct {
	OK                  bool
	ErrorCode           string
	Message             string
	NewBalanceCents     int64
	FromNewBalanceCents int64
	ToNewBalanceCents   int64
	Account             struct {
		BalanceCents int64
	}
}

type TellerRequest struct {
	RequestID   string
	AccountID   int64
	Username    string
	PercentBPS  int64
	AmountCents int64
	Operation   string
}

type TellerReply struct {
	OK              bool
	ErrorCode       string
	Message         string
	NewBalanceCents int64
	Account         struct {
		AccountID int64
		Username  string
	}
}
