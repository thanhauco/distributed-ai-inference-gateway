package openai

import (
	"math"
	"unicode"
	"unicode/utf8"
)

// perMessageOverhead approximates the chat-template tokens added around each
// message (role markers, separators).
const perMessageOverhead = 4

// EstimateTokens approximates the BPE token count of text without a model
// tokenizer. It takes the larger of a character-based (≈4 chars/token) and a
// word-based (≈0.75 words/token) estimate, which tracks Llama/Mistral
// tokenizers within ~10% on English prose and code.
func EstimateTokens(text string) int {
	if text == "" {
		return 0
	}
	chars := utf8.RuneCountInString(text)
	words, inWord := 0, false
	for _, r := range text {
		if unicode.IsSpace(r) {
			inWord = false
			continue
		}
		if unicode.IsPunct(r) || unicode.IsSymbol(r) {
			words++
			inWord = false
			continue
		}
		if !inWord {
			words++
			inWord = true
		}
	}
	byChars := int(math.Ceil(float64(chars) / 4))
	byWords := int(math.Ceil(float64(words) * 4 / 3))
	return max(byChars, byWords, 1)
}

// PromptTokens estimates the prompt tokens of a chat request.
func PromptTokens(r *ChatCompletionRequest) int {
	n := 3 // assistant priming
	for _, m := range r.Messages {
		n += perMessageOverhead + EstimateTokens(m.Content)
	}
	return n
}

// MaxCompletion returns the effective completion budget for the request.
func MaxCompletion(r *ChatCompletionRequest, fallback int) int {
	if r.MaxTokens != nil {
		return *r.MaxTokens
	}
	return fallback
}
