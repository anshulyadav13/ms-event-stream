# CLI Usage Examples

## Basic Commands

```bash
# Check system health
ms-event-stream-cli health

# Show version information
ms-event-stream-cli version

# Show configuration information
ms-event-stream-cli config

# Show system status
ms-event-stream-cli status
```

## Command Line Options

```bash
# Enable verbose logging
ms-event-stream-cli --verbose

# Output in JSON format
ms-event-stream-cli --json

# Show help
ms-event-stream-cli --help
```

## Environment Variables

```bash
# Set service name
export MS_EVENT_STREAM_SERVICE_NAME="my-service"

# Set database URL
export MS_EVENT_STREAM_DB_URL="postgresql://user:pass@localhost:5432/db"

# Set environment
export MS_EVENT_STREAM_ENV="production"

# Run CLI with environment variables
ms-event-stream-cli config
```

## Using with npm scripts

```bash
# Build and run CLI
npm run cli:build -- version

# Run CLI directly
npm run cli -- health
```

## Note

This CLI provides basic functionality for system information and health checks. For full event stream management features (inspect, replay, purge, archive), use the ms-event-stream package in your NestJS application with the EventStreamModule.
