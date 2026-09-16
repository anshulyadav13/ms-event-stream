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
  /** Originating microservice (e.g. "auth", "user", "chat"). */
  service?: string;
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
  /**
   * Call push flag (PUSH channel only). When true, the notification MS
   * routes the push as an incoming call: iOS devices are woken via an
   * APNs VoIP (PushKit) push to their registered `voipToken` with header
   * `apns-push-type: voip`, while Android devices receive a standard FCM
   * push. When false/omitted, the push is a regular FCM notification to
   * all FCM tokens.
   */
  callPush?: boolean;
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
  /** FCM token string. Optional when `voipToken` is provided. */
  token?: string;
  /**
   * APNs VoIP (PushKit) token for iOS call notifications. Optional.
   * iOS devices register BOTH `token` (FCM, for regular pushes) and
   * `voipToken` (PushKit, for incoming call pushes).
   */
  voipToken?: string;
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
  /** FCM token string to deactivate. Optional when `voipToken` is provided. */
  token?: string;
  /** APNs VoIP (PushKit) token to deactivate. Optional. */
  voipToken?: string;
}

/**
 * Payload for the notification broadcast stream
 * (StreamNames.notificationBroadcast()).
 *
 * Sent by the user MS for admin bulk/broadcast/location push sends.
 * Consumed by the notification MS and either fanned out to a list of
 * userIds or sent as an FCM topic/condition broadcast.
 */
export interface NotificationBroadcastPayload {
  /** Delivery channel. Currently only PUSH is supported. */
  channel: "PUSH" | "EMAIL";
  /** Idempotency key to prevent duplicate broadcasts. */
  idempotencyKey: string;
  /** Originating microservice (e.g. "auth", "user", "chat"). */
  service?: string;
  /** Direct notification title (bypasses template). */
  title?: string;
  /** Direct notification body (bypasses template). */
  body: string;
  /** Explicit list of target user IDs (used for bulk/location broadcasts). */
  userIds?: number[];
  /** FCM topic to broadcast to (alternative to userIds/condition). */
  topic?: string;
  /** FCM condition expression (alternative to userIds/topic). */
  condition?: string;
  /** Data payload sent with the FCM notification. */
  data?: Record<string, unknown>;
  /** Image URL for the push notification. */
  imageUrl?: string;
  /** User locale (unused for ad-hoc broadcasts, present for future use). */
  locale?: string;
  /**
   * Call push flag (PUSH channel only). When true and `userIds` are
   * provided, iOS devices are woken via an APNs VoIP (PushKit) push to
   * their registered `voipToken` (`apns-push-type: voip`) and Android
   * devices receive a standard FCM push. Ignored for topic/condition
   * broadcasts — APNs has no topic support.
   */
  callPush?: boolean;
}
