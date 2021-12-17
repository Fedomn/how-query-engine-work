package sql

type TokenStream struct {
	Tokens []Token
	cursor int
}

func (t *TokenStream) Peek() (Token, error) {
	if t.cursor < len(t.Tokens) {
		return t.Tokens[t.cursor], nil
	} else {
		return Token{}, ErrNoToken
	}
}

func (t *TokenStream) Next() (Token, error) {
	if t.cursor < len(t.Tokens) {
		return t.Tokens[t.cursor+1], nil
	} else {
		return Token{}, ErrNoToken
	}
}

func (t *TokenStream) ConsumeKeywords(s []string) bool {
	currentCursor := t.cursor
	for _, keyword := range s {
		if !t.consumeKeyword(keyword) {
			t.cursor = currentCursor
			return false
		}
	}
	return true
}

func (t *TokenStream) consumeKeyword(s string) bool {
	if peek, err := t.Peek(); err != nil {
		if peek.IsKeyword() && peek.Text == s {
			t.cursor++
			return true
		}
	}
	return false
}
