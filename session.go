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

// session returns the Flight session to send a call under, handshaking only when
// no session is established yet.
//
// The runtime answers a handshake with a session it keeps for an hour of
// inactivity, so one handshake serves every call a client makes rather than each
// one paying a round trip of its own.
func (c *SpiceClient) session(ctx context.Context) (flightSession, error) {
	if c.appId == "" || c.apiKey == "" {
		return flightSession{}, nil
	}

	c.sessionMu.Lock()
	token := c.sessionToken
	c.sessionMu.Unlock()
	if token != "" {
		return flightSession{token: token, reused: true}, nil
	}

	if c.flightClient == nil {
		return flightSession{}, fmt.Errorf("flight client is not initialized")
	}
	authCtx, err := c.flightClient.AuthenticateBasicToken(ctx, c.appId, c.apiKey)
	if err != nil {
		return flightSession{}, err
	}
	token = bearerToken(authCtx)
	if token == "" {
		return flightSession{}, fmt.Errorf("flight handshake returned no authorization token")
	}

	c.sessionMu.Lock()
	c.sessionToken = token
	c.sessionMu.Unlock()
	return flightSession{token: token, reused: false}, nil
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
