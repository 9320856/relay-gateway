package model

type EmbeddingRequest struct {
	Model          string      `json:"model"`
	Input          interface{} `json:"input"`
	User           string      `json:"user,omitempty"`
	EncodingFormat string      `json:"encoding_format,omitempty"`
	Dimensions     *int        `json:"dimensions,omitempty"`
}

type EmbeddingItem struct {
	Object    string    `json:"object"`
	Index     int       `json:"index"`
	Embedding []float64 `json:"embedding"`
}

type EmbeddingResponse struct {
	Object string          `json:"object"`
	Data   []EmbeddingItem `json:"data"`
	Model  string          `json:"model"`
	Usage  *Usage          `json:"usage,omitempty"`
}
