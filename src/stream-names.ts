/**
 * Standard stream names for all inter-service Redis Streams in the nestys
 * platform.
 *
 * NAMING STANDARD (must be followed for all new streams):
 *
 *   Stream:         {consumer-service}:{resource}:{action}:stream
 *   Consumer group:  {consumer-service}-{resource}-{action}-workers
 *   DLQ:            {consumer-service}:{resource}:{action}:dlq
 *
 * When adding a new stream:
 *   1. Add a static method here returning { stream, group, dlq }
 *   2. Add a typed payload interface in stream-payloads.ts
 *   3. Document which MS publishes and which MS consumes
 */

export interface StreamNameSet {
  /** Redis Stream key. */
  stream: string;
  /** Consumer group name. */
  group: string;
  /** Dead-letter queue stream key (poison messages). */
  dlq: string;
}

export class StreamNames {
  /**
   * Notification dispatch stream.
   *
   * Other microservices (user MS, auth MS, etc.) XADD dispatch requests to
   * this stream. The notification MS consumes via a consumer group and
   * processes each request (creates a Notification row, enqueues to the
   * PUSH/EMAIL worker).
   *
   * Publisher: user MS, auth MS (any service that needs to send a notification)
   * Consumer:  notification MS
   */
  static notificationDispatch(): StreamNameSet {
    return {
      stream: "notification:dispatch:stream",
      group: "notification-workers",
      dlq: "notification:dispatch:dlq",
    };
  }

  /**
   * FCM device token registration stream.
   *
   * The auth MS publishes to this stream when a user logs in or signs up
   * with an fcmToken. The notification MS consumes and stores the token
   * in its DeviceToken table so push notifications can be delivered.
   *
   * Publisher: auth MS (on login/signup with fcmToken)
   * Consumer:  notification MS
   */
  static notificationDeviceTokenRegister(): StreamNameSet {
    return {
      stream: "notification:device-token:register:stream",
      group: "notification-device-token-register-workers",
      dlq: "notification:device-token:register:dlq",
    };
  }

  /**
   * FCM device token removal stream.
   *
   * The auth MS publishes to this stream when a user logs out or a token
   * is explicitly removed. The notification MS consumes and deactivates
   * the token in its DeviceToken table.
   *
   * Publisher: auth MS (on logout/token removal)
   * Consumer:  notification MS
   */
  static notificationDeviceTokenRemove(): StreamNameSet {
    return {
      stream: "notification:device-token:remove:stream",
      group: "notification-device-token-remove-workers",
      dlq: "notification:device-token:remove:dlq",
    };
  }

  /**
   * Notification broadcast stream.
   *
   * The user MS publishes to this stream for admin bulk/broadcast/location
   * push sends. The notification MS consumes and either fetches device tokens
   * for the provided userIds or sends an FCM topic/condition broadcast.
   *
   * Publisher: user MS (admin bulk/broadcast messages)
   * Consumer:  notification MS
   */
  static notificationBroadcast(): StreamNameSet {
    return {
      stream: "notification:broadcast:stream",
      group: "notification-broadcast-workers",
      dlq: "notification:broadcast:dlq",
    };
  }

  /**
   * Generic method to generate stream names following the naming convention.
   *
   * This allows any microservice to create streams without updating the package.
   * Follows the convention: {consumer-service}:{resource}:{action}:stream
   *
   * @param consumerService - The microservice that consumes the stream (e.g., "media-processing", "notification")
   * @param resource - The domain resource being acted on (e.g., "video", "image", "device-token")
   * @param action - What's being done (e.g., "requested", "completed", "failed", "register")
   * @returns StreamNameSet with stream, group, and dlq names
   *
   * Example:
   *   StreamNames.custom('media-processing', 'video', 'requested')
   *   → { stream: 'media-processing:video:requested:stream',
   *       group: 'media-processing-video-requested-workers',
   *       dlq: 'media-processing:video:requested:dlq' }
   */
  static custom(consumerService: string, resource: string, action: string): StreamNameSet {
    const stream = `${consumerService}:${resource}:${action}:stream`;
    const group = `${consumerService}-${resource}-${action}-workers`;
    const dlq = `${consumerService}:${resource}:${action}:dlq`;

    return { stream, group, dlq };
  }
}
