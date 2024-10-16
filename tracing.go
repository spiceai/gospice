package gospice

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func isNoopTracerProvider(provider trace.TracerProvider) bool {
	tracer := provider.Tracer("example-tracer")
	_, span := tracer.Start(context.Background(), "example-span")

	// A NoopTracerProvider will create a no-op span that is invalid
	return !span.SpanContext().IsValid()
}

// GetOrCreateTracer adds a new tracer to the global tracer provider if one doesn't already exist
func GetOrCreateTracer(traceName string) trace.Tracer {
	tracing := otel.GetTracerProvider()

	// If no tracing provider is set, create a new one (that isn't just a noop provider)
	if isNoopTracerProvider(tracing) {
		tracing = sdktrace.NewTracerProvider()
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
		otel.SetTracerProvider(tracing)
	}

	return tracing.Tracer(traceName)
}

func (c *SpiceClient) tracer() trace.Tracer {
	return GetOrCreateTracer("github.com/spiceai/gospice")
}

func (c *SpiceClient) traceHttpRequest(ctx context.Context, spanName string, req *http.Request) context.Context {
	ctx, _ = c.tracer().Start(ctx, spanName)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	return ctx
}
