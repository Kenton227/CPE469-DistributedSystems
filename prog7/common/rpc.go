package common

const BankContainerPort string = "1234"
const BankPort string = "1234"
const BankLeaderPort string = "9001"

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
