# TODO

## 1. SpecValidationException 

> (workaround: `--skip-validate-spec`)

```sh
Exception in thread "main" org.openapitools.codegen.SpecValidationException: There were issues with the specification. The option can be disabled via validateSpec (Maven/Gradle) or --skip-validate-spec (CLI).
 | Error count: 3, Warning count: 2
Errors: 
        -attribute paths.'/v1/tools'(get).operationId is repeated
        -paths.'/v1/iceberg/namespaces/{namespace}'. Declared path parameter namespace needs to be defined as a path parameter in path or operation level
        -attribute paths.'/v1/mcp/sse'(post).parameters.[sessionId].content is missing
Warnings: 
        -attribute paths.'/v1/tools'(get).operationId is repeated
        -paths.'/v1/iceberg/namespaces/{namespace}'. Declared path parameter namespace needs to be defined as a path parameter in path or operation level
        -attribute paths.'/v1/mcp/sse'(post).parameters.[sessionId].content is missing

        at org.openapitools.codegen.config.CodegenConfigurator.toContext(CodegenConfigurator.java:718)
        at org.openapitools.codegen.config.CodegenConfigurator.toClientOptInput(CodegenConfigurator.java:745)
        at org.openapitools.codegen.cmd.Generate.execute(Generate.java:527)
        at org.openapitools.codegen.cmd.OpenApiGeneratorCommand.run(OpenApiGeneratorCommand.java:32)
        at org.openapitools.codegen.OpenAPIGenerator.main(OpenAPIGenerator.java:66)
```

## 2. OpenAPI 3.1 Support

Tried multiple Go client generators but none supported OpenAPI 3.1 well:
- **ogen** - lacks full 3.1 support
- **oapi-codegen** - lacks full 3.1 support
- **openapi-generator-cli** - works with `--skip-validate-spec`

Using `openapi-generator-cli` (Java-based) via Docker as the most compatible option.

### 2.1. Evaluate Alternative Client Generators

Consider trying other generators that may have better OpenAPI 3.1 support:
- **Fern** - https://github.com/fern-api/fern
- **Speakeasy** - https://github.com/speakeasy-api/speakeasy
- **go-swagger** - https://github.com/go-swagger/go-swagger

## 3. Generate HTTP Client

Currently only generating models (`--global-property=models`). Need to also generate a full HTTP client with:
- API methods for each endpoint
- Request/response handling
- Authentication support
- Error handling