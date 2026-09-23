package main

import "testing"

func TestByteSizeSet(t *testing.T) {
	tests := []struct {
		text string
		want int64
	}{
		{"0", 0},
		{"512", 512},
		{"512b", 512},
		{"64mb", 64 << 20},
		{"64MB", 64 << 20},
		{" 64 mb ", 64 << 20},
		{"1kb", 1 << 10},
		{"2gb", 2 << 30},
	}
	for _, tt := range tests {
		var got byteSize
		if err := got.Set(tt.text); err != nil {
			t.Errorf("Set(%q): %v", tt.text, err)
			continue
		}
		if int64(got) != tt.want {
			t.Errorf("Set(%q) = %d, want %d", tt.text, got, tt.want)
		}
	}
}

func TestByteSizeRejectsNonsense(t *testing.T) {
	for _, text := range []string{"", "mb", "-1", "-1mb", "1.5mb", "many", "9223372036854775807gb", "64 m b"} {
		var got byteSize
		if err := got.Set(text); err == nil {
			t.Errorf("Set(%q) = %d, want an error", text, got)
		}
	}
}

func TestByteSizeString(t *testing.T) {
	tests := []struct {
		size byteSize
		want string
	}{
		{0, "0b"},
		{512, "512b"},
		{1 << 10, "1kb"},
		{64 << 20, "64mb"},
		{3 << 30, "3gb"},
		{(1 << 20) + 1, "1048577b"},
	}
	for _, tt := range tests {
		if got := tt.size.String(); got != tt.want {
			t.Errorf("byteSize(%d).String() = %q, want %q", int64(tt.size), got, tt.want)
		}
	}
}
