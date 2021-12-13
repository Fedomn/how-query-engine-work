package sql

// Pratt Top Down Operator Precedence Parser. See https://tdop.github.io/ for paper.
type PrattParser interface {
	// Parse an expression
	Parse(precedence int) Expr

	// Get the precedence of the next token
	nextPrecedence() int

	// Parse the next prefix expression
	parsePrefix() Expr

	// Parse the next infix expression
	parseInfix(left Expr, precedence int) Expr
}
