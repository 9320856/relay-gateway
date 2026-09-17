package model

type APIError struct {
	Message string  `json:"message"`
	Type    string  `json:"type,omitempty"`
	Param   *string `json:"param,omitempty"`
	Code    *string `json:"code,omitempty"`
}

type ErrorResponse struct {
	Error APIError `json:"error"`
}

func NewError(message, errType string) ErrorResponse {
	return ErrorResponse{
		Error: APIError{
			Message: message,
			Type:    errType,
		},
	}
}
