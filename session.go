package gospice

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// flightSession is the credential a Flight call is sent under.
type flightSession struct {
	// token is the Authorization header value the runtime issued on the handshake,
	// "Bearer <session id>", or empty when no API key is configured.
	token string
	// reused reports whether the token came from an earlier handshake rather than
	// one performed for this call. Only a reused token can have expired, so only a
	// reused token is worth renewing when the runtime rejects it.
	reused bool
}

// apply returns ctx carrying the session's bearer token, or ctx unchanged for an
// unauthenticated (local) runtime.
func (s flightSession) apply(ctx context.Context) context.Context {
	if s.token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "Authorization", s.token)
}

// handshakeFlight is one handshake shared by every caller that wants a session
// while it runs. Its outcome -- the token, or the error -- becomes theirs, so a
// burst of callers costs one round trip whether the runtime accepts or refuses.
type handshakeFlight struct {
	// done is closed once token and err are set; they must not be read before.
	done  chan struct{}
	token string
	err   error
}

// session returns the Flight session to send a call under, handshaking only when
// no session is established yet.
//
// The runtime answers a handshake with a session it keeps for an hour of
// inactivity, so one handshake serves every call a client makes rather than each
// one paying a round trip of its own.
//
// Callers that arrive together share one handshake as well: an expiry under load
// refuses every in-flight call at nearly the same instant, and a handshake each
// would be a round trip each and a runtime session each left behind. The first
// caller through performs it and the rest take its result -- including its
// failure, so a runtime refusing the API key refuses it once rather than once
// per waiting call.
//
// A waiter honours its own ctx while it waits: the handshake belongs to the
// caller that started it, and one that hangs must not outlast the deadline of a
// call that merely arrived behind it.
func (c *SpiceClient) session(ctx context.Context) (flightSession, error) {
	if c.appId == "" || c.apiKey == "" {
		return flightSession{}, nil
	}

	c.sessionMu.Lock()
	if token := c.sessionToken; token != "" {
		c.sessionMu.Unlock()
		return flightSession{token: token, reused: true}, nil
	}
	inflight := c.handshakeFlight
	lead := inflight == nil
	if lead {
		inflight = &handshakeFlight{done: make(chan struct{})}
		c.handshakeFlight = inflight
	}
	c.sessionMu.Unlock()

	if lead {
		return c.leadHandshake(ctx, inflight)
	}

	select {
	case <-ctx.Done():
		return flightSession{}, ctx.Err()
	case <-inflight.done:
	}
	if inflight.err != nil {
		return flightSession{}, inflight.err
	}
	// Another caller's token. It is reported as reused because an arbitrary delay
	// can separate that handshake from this call -- a scheduling pause here, a
	// runtime restart there -- so it is exactly as renewable as a cached one.
	return flightSession{token: inflight.token, reused: true}, nil
}

// leadHandshake performs the handshake for inflight and publishes its outcome to
// every caller waiting on it.
func (c *SpiceClient) leadHandshake(ctx context.Context, inflight *handshakeFlight) (flightSession, error) {
	token, err := c.handshake(ctx)

	c.sessionMu.Lock()
	if err == nil {
		c.sessionToken = token
	}
	c.handshakeFlight = nil
	c.sessionMu.Unlock()

	inflight.token, inflight.err = token, err
	close(inflight.done)

	if err != nil {
		return flightSession{}, err
	}
	return flightSession{token: token, reused: false}, nil
}

// handshake authenticates with the runtime and returns the bearer token it issued.
func (c *SpiceClient) handshake(ctx context.Context) (string, error) {
	if c.flightClient == nil {
		return "", fmt.Errorf("flight client is not initialized")
	}
	authCtx, err := c.flightClient.AuthenticateBasicToken(ctx, c.appId, c.apiKey)
	if err != nil {
		return "", err
	}
	token := bearerToken(authCtx)
	if token == "" {
		return "", fmt.Errorf("flight handshake returned no authorization token")
	}
	return token, nil
}

// forgetSession drops the session holding stale, unless another call has already
// replaced it: a renewal that raced this one must not be thrown away for a third
// handshake. An empty stale drops whatever session is held.
func (c *SpiceClient) forgetSession(stale string) {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()
	if stale == "" || c.sessionToken == stale {
		c.sessionToken = ""
	}
}

// withSession runs call under the client's Flight session, renewing the session
// once when the runtime no longer recognises a reused token.
//
// A session outlives neither an hour of inactivity nor a runtime restart, and a
// client that outlives either is answered Unauthenticated until it handshakes
// again. A token this call has just obtained cannot have expired, so its
// rejection is returned as is: renewing it would turn a credential the runtime
// refuses into a loop of handshakes.
func (c *SpiceClient) withSession(ctx context.Context, call func(ctx context.Context) error) error {
	s, err := c.session(ctx)
	if err != nil {
		return err
	}
	err = call(s.apply(ctx))
	if err == nil || !s.reused || status.Code(err) != codes.Unauthenticated {
		return err
	}

	c.forgetSession(s.token)
	s, err = c.session(ctx)
	if err != nil {
		return err
	}
	return call(s.apply(ctx))
}

// bearerToken extracts the Authorization value AuthenticateBasicToken appended to
// the outgoing metadata of authCtx.
func bearerToken(authCtx context.Context) string {
	md, ok := metadata.FromOutgoingContext(authCtx)
	if !ok {
		return ""
	}
	values := md.Get("authorization")
	for i := len(values) - 1; i >= 0; i-- {
		if values[i] != "" {
			return values[i]
		}
	}
	return ""
}
