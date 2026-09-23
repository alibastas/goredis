package command

import (
	"strconv"
	"strings"
	"time"
)

// This file turns commands into the form they are stored in inside the
// append-only file.
//
// The problem it solves is that a relative timeout only means something
// at the instant it is given. "Expire in 60 seconds" written to a log and
// replayed three days later gives the key another minute of life, and a
// key that should have died long ago comes back on every restart. Redis
// solves it the same way: timeouts are rewritten to an absolute deadline
// before they are logged.
//
// A pleasant consequence is that keys expiring during normal operation do
// not have to be logged at all. Their deadline is already in the file, so
// a replay simply does not load them.

// millis formats a moment as Unix milliseconds, the way PEXPIREAT takes
// it.
func millis(t time.Time) string {
	return strconv.FormatInt(t.UnixMilli(), 10)
}

// rewriteSet replaces SET's relative timeout with an absolute one:
//
//	SET k v EX 60  ->  SET k v PXAT 1790110408000
//
// Everything else about the command is left alone. NX and XX in
// particular are kept, so a SET that was skipped when the client sent it
// is skipped again on replay, and KEEPTTL keeps working because the
// earlier commands in the log have already put that TTL back.
func rewriteSet(args []string, now time.Time) []string {
	for i := 3; i < len(args); i++ {
		unit, ok := expiryUnit(args[i])
		if !ok || i+1 == len(args) {
			continue
		}
		n, isInt := parseInt(args[i+1])
		if !isInt {
			return args
		}
		deadline, valid := deadlineOf(args[i], n, unit, now)
		if !valid {
			return args
		}
		out := append([]string(nil), args...)
		out[i], out[i+1] = "PXAT", millis(deadline)
		return out
	}
	return args
}

// expiryUnit reports the time unit a SET option counts in.
func expiryUnit(option string) (time.Duration, bool) {
	switch strings.ToUpper(option) {
	case "EX", "EXAT":
		return time.Second, true
	case "PX", "PXAT":
		return time.Millisecond, true
	}
	return 0, false
}

// deadlineOf turns an option's argument into the moment the key dies,
// whether the option counted from now (EX, PX) or from the epoch (EXAT,
// PXAT).
func deadlineOf(option string, n int64, unit time.Duration, now time.Time) (time.Time, bool) {
	if strings.HasSuffix(strings.ToUpper(option), "AT") {
		return unixTime(n, unit)
	}
	d, valid := toDuration(n, unit)
	if !valid {
		return time.Time{}, false
	}
	return now.Add(d), true
}

// rewriteExpire logs EXPIRE and PEXPIRE as the absolute PEXPIREAT:
//
//	EXPIRE k 60  ->  PEXPIREAT k 1790110408000
//
// A timeout that has already passed deletes the key, and so does a
// PEXPIREAT in the past, so that case needs no special handling.
func rewriteExpire(unit time.Duration) func([]string, time.Time) []string {
	return func(args []string, now time.Time) []string {
		n, isInt := parseInt(args[2])
		if !isInt {
			return args
		}
		d, valid := toDuration(n, unit)
		if !valid {
			return args
		}
		return []string{"PEXPIREAT", args[1], millis(now.Add(d))}
	}
}

// rewriteExpireAt logs EXPIREAT as PEXPIREAT, so replay only ever has to
// deal with one spelling of an absolute deadline.
func rewriteExpireAt(unit time.Duration) func([]string, time.Time) []string {
	return func(args []string, _ time.Time) []string {
		if unit == time.Millisecond {
			return args
		}
		n, isInt := parseInt(args[2])
		if !isInt {
			return args
		}
		deadline, valid := unixTime(n, unit)
		if !valid {
			return args
		}
		return []string{"PEXPIREAT", args[1], millis(deadline)}
	}
}
