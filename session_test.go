package gospice

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// The API key WithApiKey accepts: "<app id>|<key>". The handshake's Basic
// credential is "<app id>:<that whole string>", which is what the runtime expects.
const (
	sessionTestAppID  = "app"
	sessionTestAPIKey = "app|secret"
)

// seenRequest is one request the server admitted or refused, other than a handshake.
type seenRequest struct {
	rpc           string
	authorization string
}

// sessionTestServer is an in-process Flight server that enforces the runtime's rule:
// the handshake must carry the API key, and every later RPC must carry a bearer
// token the server still recognises, otherwise Unauthenticated.
type sessionTestServer struct {
	flight.BaseFlightServer

	mu                sync.Mutex
	handshakeAttempts int
	handshakes        int
	sessions          map[string]bool
	refuseSessions    bool
	requests          []seenRequest
}

func newSessionTestServer() *sessionTestServer {
	return &sessionTestServer{sessions: make(map[string]bool)}
}

func (s *sessionTestServer) counts() (attempts, handshakes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handshakeAttempts, s.handshakes
}

// expireSessions forgets every session the server issued, as an hour of inactivity
// or a runtime restart would.
func (s *sessionTestServer) expireSessions() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions = make(map[string]bool)
}

func header(ctx context.Context, name string) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get(name)
	if len(values) == 0 {
		return ""
	}
	return values[len(values)-1]
}

// admit mirrors the runtime's BasicAuthMiddleware.
func (s *sessionTestServer) admit(ctx context.Context, rpc string) error {
	authorization := header(ctx, "authorization")
	s.mu.Lock()
	s.requests = append(s.requests, seenRequest{rpc: rpc, authorization: authorization})
	honoured := !s.refuseSessions && s.sessions[strings.TrimPrefix(authorization, "Bearer ")]
	s.mu.Unlock()

	if authorization == "" {
		return status.Error(codes.Unauthenticated, "Missing authorization header")
	}
	if !strings.HasPrefix(authorization, "Bearer ") {
		return status.Error(codes.Unauthenticated, "Invalid authorization header")
	}
	if !honoured {
		return status.Error(codes.Unauthenticated, "Invalid credentials")
	}
	return nil
}

func (s *sessionTestServer) Handshake(stream flight.FlightService_HandshakeServer) error {
	s.mu.Lock()
	s.handshakeAttempts++
	s.mu.Unlock()

	presented := header(stream.Context(), "authorization")
	credential := base64.RawStdEncoding.EncodeToString([]byte(sessionTestAppID + ":" + sessionTestAPIKey))
	if strings.TrimRight(strings.TrimPrefix(presented, "Basic "), "=") != credential {
		return status.Error(codes.Unauthenticated, "Invalid credentials")
	}

	s.mu.Lock()
	s.handshakes++
	token := fmt.Sprintf("session-%d", s.handshakes)
	s.sessions[token] = true
	s.mu.Unlock()

	if err := stream.SetHeader(metadata.Pairs("authorization", "Bearer "+token)); err != nil {
		return err
	}
	return stream.Send(&flight.HandshakeResponse{Payload: []byte(token)})
}

func (s *sessionTestServer) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	// Sql sends the statement text as the command. Anything else is a Flight SQL
	// probe from the ADBC connection Init opens, which this server does not speak.
	if desc.Type != flight.DescriptorCMD || !strings.HasPrefix(strings.ToUpper(string(desc.Cmd)), "SELECT") {
		return nil, status.Error(codes.Unimplemented, "sessionTestServer: only SQL text commands are served")
	}
	if err := s.admit(ctx, "GetFlightInfo"); err != nil {
		return nil, err
	}
	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: []byte("results")}}},
	}, nil
}

func (s *sessionTestServer) DoGet(_ *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	if err := s.admit(stream.Context(), "DoGet"); err != nil {
		return err
	}

	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	bldr := array.NewInt32Builder(memory.DefaultAllocator)
	defer bldr.Release()
	bldr.Append(1)
	col := bldr.NewInt32Array()
	defer col.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{col}, 1)
	defer rec.Release()

	w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	if err := w.Write(rec); err != nil {
		return err
	}
	return w.Close()
}

func (s *sessionTestServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if err := s.admit(stream.Context(), "DoAction"); err != nil {
		return err
	}
	if action.Type != actionSubmitAsyncQuery {
		return status.Errorf(codes.Unimplemented, "sessionTestServer: no handler for action %s", action.Type)
	}
	return stream.Send(&flight.Result{Body: []byte(`{"query_id":"q-1","status":"PENDING"}`)})
}

func startSessionTestServer(t *testing.T, srv *sessionTestServer) string {
	t.Helper()

	fs := flight.NewServerWithMiddleware(nil)
	if err := fs.Init("127.0.0.1:0"); err != nil {
		t.Fatalf("error starting test flight server: %v", err)
	}
	fs.RegisterFlightService(srv)
	go func() { _ = fs.Serve() }()
	t.Cleanup(fs.Shutdown)

	return "grpc://" + fs.Addr().String()
}

// sessionFixture is a client against a sessionTestServer, with the server's counters
// as they stood once Init returned. Init opens an ADBC connection for SqlWithParams,
// which performs a handshake of its own, so every expectation below is relative to
// that baseline.
type sessionFixture struct {
	srv        *sessionTestServer
	spice      *SpiceClient
	attempts0  int
	handshakes int
	requests0  int
}

func newSessionFixture(t *testing.T, srv *sessionTestServer, opts ...SpiceClientModifier) *sessionFixture {
	t.Helper()

	spice := NewSpiceClient()
	opts = append([]SpiceClientModifier{WithFlightAddress(startSessionTestServer(t, srv))}, opts...)
	if err := spice.Init(opts...); err != nil {
		t.Fatalf("error initializing SpiceClient: %v", err)
	}
	t.Cleanup(func() { _ = spice.Close() })

	attempts, handshakes := srv.counts()
	srv.mu.Lock()
	requests := len(srv.requests)
	srv.mu.Unlock()
	return &sessionFixture{srv: srv, spice: spice, attempts0: attempts, handshakes: handshakes, requests0: requests}
}

// newHandshakes reports how many handshakes the client performed since Init, and
// how many of them the server accepted.
func (f *sessionFixture) newHandshakes() (attempts, accepted int) {
	a, h := f.srv.counts()
	return a - f.attempts0, h - f.handshakes
}

// token is the bearer token the k-th handshake since Init was issued.
func (f *sessionFixture) token(k int) string {
	return fmt.Sprintf("Bearer session-%d", f.handshakes+k)
}

// seen lists the rpc requests the server saw since Init.
func (f *sessionFixture) seen(rpc string) []seenRequest {
	f.srv.mu.Lock()
	defer f.srv.mu.Unlock()
	var out []seenRequest
	for _, r := range f.srv.requests[f.requests0:] {
		if r.rpc == rpc {
			out = append(out, r)
		}
	}
	return out
}

func countRows(t *testing.T, rdr array.RecordReader) int {
	t.Helper()
	defer rdr.Release()
	rows := 0
	for rdr.Next() {
		rows += int(rdr.RecordBatch().NumRows())
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("error reading results: %v", err)
	}
	return rows
}

func assertAuthorization(t *testing.T, requests []seenRequest, want ...string) {
	t.Helper()
	if len(requests) != len(want) {
		t.Fatalf("expected %d requests, saw %d: %+v", len(want), len(requests), requests)
	}
	for i, r := range requests {
		if r.authorization != want[i] {
			t.Fatalf("request %d (%s): expected authorization %q, got %q", i, r.rpc, want[i], r.authorization)
		}
	}
}

// One handshake serves every query a client makes: the runtime keeps the session,
// so a second handshake per query is a round trip that buys nothing.
func TestSqlHandshakesOnceAndReusesTheSession(t *testing.T) {
	f := newSessionFixture(t, newSessionTestServer(), WithApiKey(sessionTestAPIKey))
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		rdr, err := f.spice.Sql(ctx, "SELECT 1")
		if err != nil {
			t.Fatalf("query %d: %v", i, err)
		}
		if rows := countRows(t, rdr); rows != 1 {
			t.Fatalf("query %d: expected 1 row, got %d", i, rows)
		}
	}

	if _, handshakes := f.newHandshakes(); handshakes != 1 {
		t.Fatalf("five queries must share one handshake, got %d", handshakes)
	}
	assertAuthorization(t, f.seen("DoGet"), f.token(1), f.token(1), f.token(1), f.token(1), f.token(1))
}

// The async actions share the session with Sql rather than handshaking per call.
func TestAsyncActionsShareTheSession(t *testing.T) {
	f := newSessionFixture(t, newSessionTestServer(), WithApiKey(sessionTestAPIKey))
	ctx := context.Background()

	rdr, err := f.spice.Sql(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	countRows(t, rdr)

	for i := 0; i < 3; i++ {
		if _, err := f.spice.Query(ctx, "SELECT 1"); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	if _, handshakes := f.newHandshakes(); handshakes != 1 {
		t.Fatalf("Sql and three submissions must share one handshake, got %d", handshakes)
	}
	assertAuthorization(t, f.seen("DoAction"), f.token(1), f.token(1), f.token(1))
}

// A session the runtime has forgotten is renewed once, transparently, and the
// query that found it expired still succeeds.
func TestAnExpiredSessionIsRenewedOnce(t *testing.T) {
	f := newSessionFixture(t, newSessionTestServer(), WithApiKey(sessionTestAPIKey))
	ctx := context.Background()

	rdr, err := f.spice.Sql(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("first query: %v", err)
	}
	countRows(t, rdr)

	f.srv.expireSessions()

	rdr, err = f.spice.Sql(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("an expired session must be renewed, not surfaced: %v", err)
	}
	if rows := countRows(t, rdr); rows != 1 {
		t.Fatalf("expected 1 row after renewal, got %d", rows)
	}
	if _, handshakes := f.newHandshakes(); handshakes != 2 {
		t.Fatalf("expected exactly one renewal handshake, got %d handshakes", handshakes)
	}
	// The stale token is presented once, refused, and replaced.
	assertAuthorization(t, f.seen("GetFlightInfo"), f.token(1), f.token(1), f.token(2))

	// The renewed session serves the async actions too.
	if _, err := f.spice.Query(ctx, "SELECT 1"); err != nil {
		t.Fatalf("submit after renewal: %v", err)
	}
	if _, handshakes := f.newHandshakes(); handshakes != 2 {
		t.Fatalf("the renewed session must be reused, got %d handshakes", handshakes)
	}
}

// An async action renews an expired session the same way.
func TestAnAsyncActionRenewsAnExpiredSession(t *testing.T) {
	f := newSessionFixture(t, newSessionTestServer(), WithApiKey(sessionTestAPIKey))
	ctx := context.Background()

	if _, err := f.spice.Query(ctx, "SELECT 1"); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	f.srv.expireSessions()
	if _, err := f.spice.Query(ctx, "SELECT 1"); err != nil {
		t.Fatalf("submit after expiry must renew: %v", err)
	}
	if _, handshakes := f.newHandshakes(); handshakes != 2 {
		t.Fatalf("expected one renewal, got %d handshakes", handshakes)
	}
	assertAuthorization(t, f.seen("DoAction"), f.token(1), f.token(1), f.token(2))
}

// A credential the runtime refuses is refused once. Renewing it would handshake in
// a loop against the server.
func TestARejectedApiKeyIsNotRetried(t *testing.T) {
	f := newSessionFixture(t, newSessionTestServer(), WithApiKey("app|not-the-key"))

	_, err := f.spice.Sql(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("a refused credential must fail the query")
	}
	if status.Code(err) != codes.Unauthenticated || !strings.Contains(err.Error(), "Invalid credentials") {
		t.Fatalf("the runtime's explanation must reach the caller, got: %v", err)
	}
	attempts, handshakes := f.newHandshakes()
	if attempts != 1 || handshakes != 0 {
		t.Fatalf("expected one refused handshake, got %d attempts and %d sessions", attempts, handshakes)
	}
	if len(f.seen("GetFlightInfo")) != 0 {
		t.Fatal("no query may be sent without a session")
	}
}

// A token the runtime issued and immediately refuses cannot have expired, so it is
// not renewed: one handshake, one refusal, and the error reaches the caller.
func TestAFreshSessionTheRuntimeRefusesIsNotRenewed(t *testing.T) {
	srv := newSessionTestServer()
	srv.refuseSessions = true
	f := newSessionFixture(t, srv, WithApiKey(sessionTestAPIKey))
	ctx := context.Background()

	_, err := f.spice.Sql(ctx, "SELECT 1")
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got: %v", err)
	}
	if _, handshakes := f.newHandshakes(); handshakes != 1 {
		t.Fatalf("a fresh token is not renewed, got %d handshakes", handshakes)
	}
	if got := len(f.seen("GetFlightInfo")); got != 1 {
		t.Fatalf("expected one refused GetFlightInfo, got %d", got)
	}

	_, err = f.spice.Query(ctx, "SELECT 1")
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got: %v", err)
	}
	if _, handshakes := f.newHandshakes(); handshakes != 2 {
		t.Fatalf("the reused token is renewed once, and the renewed one is not; got %d handshakes", handshakes)
	}
}

// Without an API key there is no handshake and nothing to renew.
func TestNoApiKeyMeansNoHandshake(t *testing.T) {
	f := newSessionFixture(t, newSessionTestServer())

	_, err := f.spice.Sql(context.Background(), "SELECT 1")
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("the server requires a session, expected Unauthenticated, got: %v", err)
	}
	if attempts, _ := f.newHandshakes(); attempts != 0 {
		t.Fatalf("no api key, no handshake; got %d attempts", attempts)
	}
}
