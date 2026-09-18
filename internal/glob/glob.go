// Package glob implements the glob-style patterns Redis uses in KEYS and
// PSUBSCRIBE:
//
//   - "*" matches any sequence of bytes, including an empty one
//   - "?" matches exactly one byte
//   - "[abc]" matches one byte from the set; "[a-z]" ranges and "[^abc]"
//     negation work too
//   - "\x" matches the byte x literally, so "\*" matches a star
//
// Unlike path.Match, '/' has no special meaning, which is what Redis users
// expect for keys like "user:1/profile".
package glob

// Match reports whether s matches pattern. It works on bytes rather than
// runes, like Redis does.
//
// Stars are handled by remembering the position of the last star seen and
// retrying from there when a later part of the pattern fails. This keeps
// the running time at O(len(pattern) * len(s)). A naive recursive matcher
// can take exponential time on patterns like "*a*a*a*a*b".
func Match(pattern, s string) bool {
	p, i := 0, 0
	// Position of the most recent star in the pattern, and the position in
	// s that the star is currently assumed to extend to.
	starP, starI := -1, 0

	for i < len(s) {
		if p < len(pattern) {
			switch pattern[p] {
			case '*':
				starP, starI = p, i
				p++
				continue
			case '?':
				p++
				i++
				continue
			case '[':
				if ok, next := matchClass(pattern, p, s[i]); ok {
					p = next
					i++
					continue
				}
			case '\\':
				if lit, next := escaped(pattern, p); lit == s[i] {
					p = next
					i++
					continue
				}
			default:
				if pattern[p] == s[i] {
					p++
					i++
					continue
				}
			}
		}

		// Mismatch. If there was an earlier star, let it swallow one more
		// byte and try the rest of the pattern again from there.
		if starP < 0 {
			return false
		}
		starI++
		i = starI
		p = starP + 1
	}

	// s is used up. What remains of the pattern must only be stars.
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}

// escaped returns the literal byte for a backslash at pattern[p] and the
// index after it. A trailing backslash matches itself.
func escaped(pattern string, p int) (lit byte, next int) {
	if p+1 < len(pattern) {
		return pattern[p+1], p + 2
	}
	return '\\', p + 1
}

// matchClass matches c against the bracket expression starting at
// pattern[p] == '['. It returns whether c matched and the index just past
// the closing ']'. A '[' without a closing ']' is treated as a literal.
func matchClass(pattern string, p int, c byte) (matched bool, next int) {
	i := p + 1
	negate := false
	if i < len(pattern) && pattern[i] == '^' {
		negate = true
		i++
	}

	for {
		if i >= len(pattern) {
			return c == '[', p + 1
		}
		ch := pattern[i]
		switch {
		case ch == ']':
			return matched != negate, i + 1
		case ch == '\\' && i+1 < len(pattern):
			if pattern[i+1] == c {
				matched = true
			}
			i += 2
		case i+2 < len(pattern) && pattern[i+1] == '-' && pattern[i+2] != ']':
			lo, hi := ch, pattern[i+2]
			if lo > hi {
				lo, hi = hi, lo
			}
			if lo <= c && c <= hi {
				matched = true
			}
			i += 3
		default:
			if ch == c {
				matched = true
			}
			i++
		}
	}
}
