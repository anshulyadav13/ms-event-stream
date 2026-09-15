/**
 * Typed payload interfaces for each Redis Stream.
 *
 * Each interface corresponds to a stream defined in stream-names.ts.
 * The StreamBusService serializes these to flat string fields (JSON.stringify
 * for complex values) when publishing, and deserializes them back when
 * consuming.
 *
 * When adding a new payload:
 *   1. All primitive fields (string, number, boolean) are stored as-is
 *      (converted to string)
 *   2. Complex fields (objects, arrays) are JSON.stringify'd on publish and
 *      JSON.parse'd on consume
 *   3. Optional fields are omitted from the stream entry if not provided
 *   4. The interface must be exported from index.ts
 */

/**
 * Payload for the notification dispatch stream
 * (StreamNames.notificationDispatch()).
 *
 * Sent by any microservice that needs to trigger a notification.
 * Consumed by the notification MS DispatchService.
 */
export interface NotificationDispatchPayload {
  /** Target user ID. */
  userId: number;
  /** Delivery channel. */
  channel: "PUSH" | "EMAIL";
  /** Idempotency key to prevent duplicate dispatches. */
  idempotencyKey: string;
  /** Template ID to render (optional if title/body are provided directly). */
  templateId?: string;
  /** Template version (optional, defaults to latest). */
  templateVer?: number;
  /** User locale for template rendering. */
  locale?: string;
  /** Template parameters (JSON-serialized in stream). */
  params?: Record<string, unknown>;
  /** Direct title (bypasses template). */
  title?: string;
  /** Direct body (bypasses template). */
  body?: string;
  /** Direct recipient email for EMAIL channel. */
  toEmail?: string;
  /**
   * Image URL for push notifications (PUSH channel only). When provided,
   * the notification MS builds platform-specific FCM configs so the image
   * is displayed in the notification on Android (large icon), iOS (via
   * Notification Service Extension + mutable-content), and Web (image).
   * Must be a publicly accessible HTTPS URL.
   */
  imageUrl?: string;
}

/**
 * Payload for the device token registration stream
 * (StreamNames.notificationDeviceTokenRegister()).
 *
 * Sent by the auth MS when a user logs in or signs up with an FCM token.
 * Consumed by the notification MS DeviceTokensService.register().
 */
export interface DeviceTokenRegisterPayload {
  /** Target user ID. */
  userId: number;
  /** FCM token string. */
  token: string;
  /** Unique device identifier. */
  deviceId?: string;
  /** Device platform: "android", "ios", or "web". */
  platform?: string;
  /** Device type/model (optional). */
  deviceType?: string;
}

/**
 * Payload for the device token removal stream
 * (StreamNames.notificationDeviceTokenRemove()).
 *
 * Sent by the auth MS when a user logs out or a token is removed.
 * Consumed by the notification MS DeviceTokensService.deactivate().
 */
export interface DeviceTokenRemovePayload {
  /** Target user ID. */
  userId: number;
  /** FCM token string to deactivate. */
  token: string;
}
