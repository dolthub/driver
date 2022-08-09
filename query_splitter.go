package embedded

import (
	"io"
	"strings"
)

var openRunes = map[rune]bool{
	'(':  true,
	'"':  true,
	'\'': true,
	'`':  true,
}

// RuneStack is a simple stack of runes
type RuneStack struct {
	chars []rune
}

// NewByteStack returns a new RuneStack object
func NewByteStack() *RuneStack {
	return &RuneStack{chars: make([]rune, 0, 64)}
}

// Push pushes a new rune on the stack
func (bs *RuneStack) Push(b rune) {
	bs.chars = append(bs.chars, b)
}

// Pop takes the top value of the top of the stack and returns it
func (bs *RuneStack) Pop() rune {
	l := len(bs.chars)

	if l == 0 {
		return 0
	}

	ch := bs.chars[l-1]
	bs.chars = bs.chars[:l-1]
	return ch
}

// Peek returns the value at the top of the stack
func (bs *RuneStack) Peek() rune {
	l := len(bs.chars)

	if l == 0 {
		return 0
	}

	return bs.chars[l-1]
}

type QuerySplitter struct {
	queries string
	pos     int
}

func NewQuerySplitter(str string) *QuerySplitter {
	return &QuerySplitter{
		queries: str,
		pos:     0,
	}
}

func (qs *QuerySplitter) Next() (string, error) {
	if qs.pos >= len(qs.queries) {
		return "", io.EOF
	}

	n, err := parseNext(qs.queries[qs.pos:])
	if err != nil {
		return "", err
	}

	nextQuery := strings.TrimSpace(qs.queries[qs.pos : qs.pos+n])
	qs.pos += n

	return nextQuery, nil
}

func (qs *QuerySplitter) HasMore() bool {
	return qs.pos < len(qs.queries)
}

func parseNext(queries string) (int, error) {
	openStack := NewByteStack()

	var prevCh rune
	for pos, ch := range queries {
		lastOpen := openStack.Peek()
		switch lastOpen {
		case 0:
			if openRunes[ch] {
				openStack.Push(ch)
			} else if ch == ';' {
				return pos + 1, nil
			}
		case '"', '\'', '`':
			if ch == lastOpen && prevCh != '\\' {
				openStack.Pop()
			}
		case '(':
			if ch == ')' {
				openStack.Pop()
			} else if openRunes[ch] {
				openStack.Push(ch)
			}
		}

		prevCh = ch
	}

	return len(queries), nil
}
