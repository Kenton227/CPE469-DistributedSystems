package common

const BankPort string = "9000"

type CustomerRequest struct {
	ActorAccountID 	int64
	TargetAccountID int64
	AmountCents 	int64
}

type CustomerReply struct {
	OK                  bool
	Message             string
}

type TellerRequest struct {
	Username		string
	TargetAccountID	int64
	PercentBPS  	int64
	AmountCents 	int64
}

type TellerReply struct {
	OK              bool
	Message         string
}

type GetIDRequest struct {
	Username		string
}

type GetIDReply struct {
	OK			bool
	ErrorMsg	string
	AccountID	int64
}
