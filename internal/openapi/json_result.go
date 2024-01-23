package openapi

type JSONResult struct {
	Code       int
	ResultCode string // result code
	Success    bool   // if success, for client
	Message    string // error message
	RequestId  string // request unique id
	Data       interface{}
}
