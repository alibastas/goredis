// Command goredis-benchmark measures a running goredis (or Redis) server,
// in the spirit of redis-benchmark: many clients send a fixed number of
// requests over real TCP connections, and it reports throughput and
// latency percentiles for each command.
//
//	goredis-benchmark -c 50 -n 100000 -P 16 -t set,get
package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alibastas/goredis/internal/resp"
)

type config struct {
	addr     string
	clients  int
	requests int
	pipeline int
	keyspace int
	value    string
}

// commandMaker builds the arguments of the next request. Each client has
// its own random source, so clients don't contend on a shared one.
type commandMaker func(rng *rand.Rand, cfg config) []string

var tests = map[string]commandMaker{
	"ping": func(*rand.Rand, config) []string { return []string{"PING"} },
	"set": func(rng *rand.Rand, cfg config) []string {
		return []string{"SET", randomKey(rng, cfg), cfg.value}
	},
	"get": func(rng *rand.Rand, cfg config) []string {
		return []string{"GET", randomKey(rng, cfg)}
	},
	// INCR gets its own keys: the ones SET wrote hold non-numeric values.
	"incr": func(rng *rand.Rand, cfg config) []string {
		return []string{"INCR", "counter:" + strconv.Itoa(rng.IntN(cfg.keyspace))}
	},
	// All clients push to the same list: a single hot key, which sharding
	// can't help with.
	"lpush": func(_ *rand.Rand, cfg config) []string {
		return []string{"LPUSH", "bench:list", cfg.value}
	},
}

func randomKey(rng *rand.Rand, cfg config) string {
	return "key:" + strconv.Itoa(rng.IntN(cfg.keyspace))
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "goredis-benchmark:", err)
		os.Exit(1)
	}
}

func run() error {
	host := flag.String("h", "127.0.0.1", "server host")
	port := flag.Int("p", 6380, "server port")
	clients := flag.Int("c", 50, "number of parallel connections")
	requests := flag.Int("n", 100_000, "total requests per test")
	pipeline := flag.Int("P", 1, "requests sent per round trip (pipelining)")
	keyspace := flag.Int("r", 100_000, "number of distinct keys to use")
	dataSize := flag.Int("d", 3, "value size in bytes for SET and LPUSH")
	testList := flag.String("t", "ping,set,get,incr,lpush", "comma-separated tests to run")
	flag.Parse()

	cfg := config{
		addr:     net.JoinHostPort(*host, strconv.Itoa(*port)),
		clients:  *clients,
		requests: *requests,
		pipeline: *pipeline,
		keyspace: *keyspace,
		value:    strings.Repeat("x", *dataSize),
	}
	if cfg.clients < 1 || cfg.requests < cfg.clients || cfg.pipeline < 1 || cfg.keyspace < 1 {
		return fmt.Errorf("need -c >= 1, -n >= -c, -P >= 1 and -r >= 1")
	}

	fmt.Printf("%d clients, %d requests per test, pipeline %d, %s\n\n", cfg.clients, cfg.requests, cfg.pipeline, cfg.addr)
	for _, name := range strings.Split(*testList, ",") {
		name = strings.ToLower(strings.TrimSpace(name))
		mk, ok := tests[name]
		if !ok {
			return fmt.Errorf("unknown test %q", name)
		}
		res, err := runTest(cfg, mk)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		fmt.Println(res.summary(strings.ToUpper(name)))
	}
	return nil
}

type result struct {
	requests  int
	errors    int
	elapsed   time.Duration
	latencies []time.Duration // one per request, sorted
}

func (r result) throughput() float64 {
	return float64(r.requests) / r.elapsed.Seconds()
}

func (r result) summary(name string) string {
	s := fmt.Sprintf("%-6s %12.0f req/s   p50 %8s   p99 %8s   max %8s",
		name, r.throughput(),
		formatLatency(percentile(r.latencies, 50)),
		formatLatency(percentile(r.latencies, 99)),
		formatLatency(percentile(r.latencies, 100)))
	if r.errors > 0 {
		s += fmt.Sprintf("   (%d error replies)", r.errors)
	}
	return s
}

// runTest opens cfg.clients connections and has each send its share of
// the requests. Connections are opened before the clock starts, so only
// request traffic is measured.
func runTest(cfg config, mk commandMaker) (result, error) {
	conns := make([]net.Conn, cfg.clients)
	for i := range conns {
		conn, err := net.Dial("tcp", cfg.addr)
		if err != nil {
			return result{}, err
		}
		defer conn.Close()
		conns[i] = conn
	}

	type clientResult struct {
		latencies []time.Duration
		errors    int
		err       error
	}
	results := make([]clientResult, cfg.clients)

	var wg sync.WaitGroup
	start := time.Now()
	for i, conn := range conns {
		// Spread the remainder so the total is exactly cfg.requests.
		share := cfg.requests / cfg.clients
		if i < cfg.requests%cfg.clients {
			share++
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			lat, errs, err := runClient(conn, cfg, mk, share)
			results[i] = clientResult{lat, errs, err}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	res := result{requests: cfg.requests, elapsed: elapsed}
	for _, r := range results {
		if r.err != nil {
			return result{}, r.err
		}
		res.errors += r.errors
		res.latencies = append(res.latencies, r.latencies...)
	}
	slices.Sort(res.latencies)
	return res, nil
}

// runClient sends n requests over conn in batches of cfg.pipeline. Every
// request in a batch is counted with the batch's round-trip time, since
// that is how long its caller would have waited.
func runClient(conn net.Conn, cfg config, mk commandMaker, n int) (latencies []time.Duration, errorReplies int, err error) {
	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)
	rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	latencies = make([]time.Duration, 0, n)

	for sent := 0; sent < n; {
		batch := min(cfg.pipeline, n-sent)

		begin := time.Now()
		for range batch {
			if err := w.WriteValue(request(mk(rng, cfg))); err != nil {
				return nil, 0, err
			}
		}
		if err := w.Flush(); err != nil {
			return nil, 0, err
		}
		for range batch {
			reply, err := r.ReadValue()
			if err != nil {
				return nil, 0, err
			}
			if reply.Type == resp.Error {
				errorReplies++
			}
		}
		took := time.Since(begin)

		for range batch {
			latencies = append(latencies, took)
		}
		sent += batch
	}
	return latencies, errorReplies, nil
}

func request(args []string) resp.Value {
	elems := make([]resp.Value, len(args))
	for i, a := range args {
		elems[i] = resp.NewBulkString(a)
	}
	return resp.NewArray(elems...)
}

// percentile returns the p-th percentile of sorted, using the
// nearest-rank method: the smallest value that at least p percent of the
// samples are less than or equal to.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	return sorted[max(rank-1, 0)]
}

func formatLatency(d time.Duration) string {
	return fmt.Sprintf("%.3fms", float64(d)/float64(time.Millisecond))
}
