package glob

import (
	"strings"
	"testing"
	"time"
)

func TestMatch(t *testing.T) {
	tests := []struct {
		pattern, s string
		want       bool
	}{
		{"", "", true},
		{"", "a", false},
		{"*", "", true},
		{"*", "anything at all", true},
		{"**", "abc", true},
		{"user:*", "user:42", true},
		{"user:*", "user:", true},
		{"user:*", "session:42", false},
		{"*:profile", "user:1:profile", true},
		{"*/*", "a/b", true},
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		{"h*llo", "heeeello", true},
		{"h*llo", "hellop", false},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"h[a-b]llo", "hbllo", true},
		{"h[a-b]llo", "hcllo", false},
		{"h[b-a]llo", "hallo", true}, // reversed ranges work like Redis
		{"[\\]]", "]", true},
		{"[a-]", "-", true},
		{"h\\*llo", "h*llo", true},
		{"h\\*llo", "hello", false},
		{"trailing\\", "trailing\\", true},
		{"[abc", "[abc", true}, // unclosed bracket is a literal '['
		{"*a*b*c", "xxaxxbxxc", true},
		{"*a*b*c", "xxaxxbxx", false},
	}

	for _, tt := range tests {
		if got := Match(tt.pattern, tt.s); got != tt.want {
			t.Errorf("Match(%q, %q) = %v, want %v", tt.pattern, tt.s, got, tt.want)
		}
	}
}

// A backtracking matcher takes exponential time on this input. Ours must
// finish quickly.
func TestMatchPathologicalPattern(t *testing.T) {
	pattern := strings.Repeat("*a", 30) + "b"
	s := strings.Repeat("a", 100)

	start := time.Now()
	if Match(pattern, s) {
		t.Fatal("pattern should not match")
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("took %v, matcher is probably backtracking exponentially", elapsed)
	}
}

func FuzzMatch(f *testing.F) {
	f.Add("h[^e]l*o\\", "hallo")
	f.Add("[a-", "b")
	f.Fuzz(func(t *testing.T, pattern, s string) {
		Match(pattern, s) // must not panic
		if !Match("*", s) {
			t.Fatalf("* must match %q", s)
		}
		if !Match(escapeAll(s), s) {
			t.Fatalf("an escaped copy of %q must match itself", s)
		}
	})
}

func escapeAll(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		b.WriteByte('\\')
		b.WriteByte(s[i])
	}
	return b.String()
}
