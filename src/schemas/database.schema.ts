/**
 * Database schema definitions for event system tables
 * These schemas define the structure for outbox and inbox patterns
 */

/**
 * Outbox events table schema
 * Stores events that need to be published to ensure reliability
 */
export const OUTBOX_EVENTS_SCHEMA = `
  CREATE TABLE IF NOT EXISTS outbox_events (
    event_id VARCHAR(100) PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    service_name VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    retry_count INTEGER DEFAULT 0,
    last_retry_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT
  );
`;

/**
 * Inbox events table schema  
 * Stores consumed events for deduplication and processing tracking
 */
export const INBOX_EVENTS_SCHEMA = `
  CREATE TABLE IF NOT EXISTS inbox_events (
    event_id VARCHAR(100) PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'received',
    service_name VARCHAR(50) NOT NULL,
    received_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0
  );
`;

/**
 * Archived outbox events table schema
 * For storing old outbox events that have been archived
 */
export const OUTBOX_EVENTS_ARCHIVED_SCHEMA = `
  CREATE TABLE IF NOT EXISTS outbox_events_archived (
    event_id VARCHAR(100) PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL,
    service_name VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    retry_count INTEGER NOT NULL,
    last_retry_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    archived_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
  );
`;

/**
 * Archived inbox events table schema
 * For storing old inbox events that have been archived
 */
export const INBOX_EVENTS_ARCHIVED_SCHEMA = `
  CREATE TABLE IF NOT EXISTS inbox_events_archived (
    event_id VARCHAR(100) PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL,
    service_name VARCHAR(50) NOT NULL,
    received_at TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    retry_count INTEGER NOT NULL,
    archived_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
  );
`;

/**
 * Event system metadata table
 * Stores system-level information like schema version, service registration, etc.
 */
export const EVENT_SYSTEM_METADATA_SCHEMA = `
  CREATE TABLE IF NOT EXISTS event_system_metadata (
    key VARCHAR(100) PRIMARY KEY,
    value TEXT NOT NULL,
    service_name VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
  );
`;

/**
 * All schema definitions in order of dependency
 */
export const ALL_SCHEMAS = [
  EVENT_SYSTEM_METADATA_SCHEMA,
  OUTBOX_EVENTS_SCHEMA,
  INBOX_EVENTS_SCHEMA,
  OUTBOX_EVENTS_ARCHIVED_SCHEMA,
  INBOX_EVENTS_ARCHIVED_SCHEMA,
];

/**
 * Event status enumeration
 */
export enum EventStatus {
  // Outbox statuses
  PENDING = 'pending',
  SENT = 'sent',
  FAILED = 'failed',
  
  // Inbox statuses  
  RECEIVED = 'received',
  PROCESSED = 'processed',
}

/**
 * Database table names
 */
export const TABLE_NAMES = {
  OUTBOX_EVENTS: 'outbox_events',
  INBOX_EVENTS: 'inbox_events', 
  OUTBOX_EVENTS_ARCHIVED: 'outbox_events_archived',
  INBOX_EVENTS_ARCHIVED: 'inbox_events_archived',
  EVENT_SYSTEM_METADATA: 'event_system_metadata',
} as const;

/**
 * Database indexes for performance optimization
 */
export const PERFORMANCE_INDEXES = {
  // Basic indexes for outbox_events
  OUTBOX_STATUS: 'CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_events(status);',
  OUTBOX_CREATED_AT: 'CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON outbox_events(created_at);',
  OUTBOX_SERVICE_NAME: 'CREATE INDEX IF NOT EXISTS idx_outbox_service_name ON outbox_events(service_name);',
  OUTBOX_EVENT_TYPE: 'CREATE INDEX IF NOT EXISTS idx_outbox_event_type ON outbox_events(event_type);',
  
  // Basic indexes for inbox_events
  INBOX_STATUS: 'CREATE INDEX IF NOT EXISTS idx_inbox_status ON inbox_events(status);',
  INBOX_RECEIVED_AT: 'CREATE INDEX IF NOT EXISTS idx_inbox_received_at ON inbox_events(received_at);',
  INBOX_SERVICE_NAME: 'CREATE INDEX IF NOT EXISTS idx_inbox_service_name ON inbox_events(service_name);',
  INBOX_EVENT_TYPE: 'CREATE INDEX IF NOT EXISTS idx_inbox_event_type ON inbox_events(event_type);',
  
  // Composite indexes for common query patterns
  OUTBOX_STATUS_CREATED: 'CREATE INDEX IF NOT EXISTS idx_outbox_status_created ON outbox_events(status, created_at);',
  INBOX_STATUS_RECEIVED: 'CREATE INDEX IF NOT EXISTS idx_inbox_status_received ON inbox_events(status, received_at);',
  
  // Partial indexes for frequently queried subsets
  OUTBOX_PENDING: 'CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events(created_at) WHERE status = \'pending\';',
  INBOX_UNPROCESSED: 'CREATE INDEX IF NOT EXISTS idx_inbox_unprocessed ON inbox_events(received_at) WHERE status = \'received\';',
  
  // Archived table indexes
  OUTBOX_ARCHIVED_SERVICE_NAME: 'CREATE INDEX IF NOT EXISTS idx_outbox_archived_service_name ON outbox_events_archived(service_name);',
  OUTBOX_ARCHIVED_EVENT_TYPE: 'CREATE INDEX IF NOT EXISTS idx_outbox_archived_event_type ON outbox_events_archived(event_type);',
  OUTBOX_ARCHIVED_CREATED_AT: 'CREATE INDEX IF NOT EXISTS idx_outbox_archived_created_at ON outbox_events_archived(created_at);',
  OUTBOX_ARCHIVED_ARCHIVED_AT: 'CREATE INDEX IF NOT EXISTS idx_outbox_archived_archived_at ON outbox_events_archived(archived_at);',
  
  INBOX_ARCHIVED_SERVICE_NAME: 'CREATE INDEX IF NOT EXISTS idx_inbox_archived_service_name ON inbox_events_archived(service_name);',
  INBOX_ARCHIVED_EVENT_TYPE: 'CREATE INDEX IF NOT EXISTS idx_inbox_archived_event_type ON inbox_events_archived(event_type);',
  INBOX_ARCHIVED_RECEIVED_AT: 'CREATE INDEX IF NOT EXISTS idx_inbox_archived_received_at ON inbox_events_archived(received_at);',
  INBOX_ARCHIVED_ARCHIVED_AT: 'CREATE INDEX IF NOT EXISTS idx_inbox_archived_archived_at ON inbox_events_archived(archived_at);',
} as const;
