package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
)

// SynthGenerator produces identical HTTP request payloads for saturation testing.
// The body is pre-built once and reused across all requests; only the req_id differs.
type SynthGenerator struct {
	model    string
	body     []byte
	endpoint string
	inputLen int
	outLen   int
	counter  atomic.Uint64
}

func NewSynthGenerator(model string, promptWords, outputTokens int, stream bool) *SynthGenerator {
	prompt := strings.Repeat("word ", promptWords)
	payload := chatPayloadDeterministic{
		Model:       model,
		Messages:    []chatMessage{{Role: "user", Content: prompt}},
		MaxTokens:   outputTokens,
		MinTokens:   outputTokens,
		Temperature: 0.7,
		IgnoreEOS:   true,
		Stream:      stream,
	}
	body, _ := json.Marshal(payload)
	return &SynthGenerator{
		model:    model,
		body:     body,
		endpoint: "/v1/chat/completions",
		inputLen: promptWords,
		outLen:   outputTokens,
	}
}

func (sg *SynthGenerator) Next() preparedRequest {
	id := sg.counter.Add(1)
	return preparedRequest{
		traceRequest: traceRequest{
			ReqID:    fmt.Sprintf("sat_%d", id),
			Model:    sg.model,
			Mode:     "chat",
			Prompt:   "",
			InputLen: sg.inputLen,
			OutputLen: sg.outLen,
		},
		body:     sg.body,
		endpoint: sg.endpoint,
	}
}
