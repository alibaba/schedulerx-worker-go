package openapi

import "encoding/json"

type HttpJobConfig struct {
	JobBaseConfig

	httpAttribute HttpAttribute
}

func NewDefaultHTTPJobConfig(className string) HttpJobConfig {
	cfg := HttpJobConfig{
		JobBaseConfig: NewDefaultJobBaseConfig(),
		httpAttribute: NewDefaultHttpAttribute(),
	}
	cfg.JobBaseConfig.executeMode = ExecModeStandalone

	type Content struct {
		ClassName string `json:"className"`
	}
	c := Content{
		ClassName: className,
	}
	contentData, _ := json.Marshal(c)

	cfg.JobBaseConfig.paramMap = map[string]interface{}{
		"jobType":     "http",
		"content":     string(contentData),
		"contentType": "text",
	}
	return cfg
}

func (c *HttpJobConfig) IsRequired() bool {
	return c.JobBaseConfig.IsRequired() && c.httpAttribute.IsRequired()
}

type HttpAttribute struct {
	// Full url link
	url string
	// Default GET request
	method string
	// Optional fields
	cookie string
	// The request successfully parses the return value key
	respKey string
	// The request successfully parses the return value corresponding to value
	respValue string
	// Request timeout in seconds, maximum 15s
	timeout int64
}

func NewDefaultHttpAttribute() HttpAttribute {
	return HttpAttribute{
		method: HTTPGetMethod,
	}
}

func (attr *HttpAttribute) IsRequired() bool {
	return attr.url != "" && attr.method != "" && attr.respKey != "" && attr.respValue != "" && attr.timeout > 0
}
