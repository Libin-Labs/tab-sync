export type MessageMap = Record<string, unknown>;

export type SyncMessage<Messages extends MessageMap> = {
  [Type in Extract<keyof Messages, string>]: {
    id: string;
    type: Type;
    payload: Messages[Type];
    sender: string;
    sentAt: number;
  };
}[Extract<keyof Messages, string>];

export type MessageTransport = {
  postMessage(message: unknown): void;
  addEventListener(type: 'message', listener: (event: { data: unknown }) => void): void;
  removeEventListener(type: 'message', listener: (event: { data: unknown }) => void): void;
  close(): void;
};

export type TabSyncOptions = {
  instanceId?: string;
  transportFactory?: (channelName: string) => MessageTransport;
  now?: () => number;
};

export class TabSyncError extends Error {
  override name = 'TabSyncError';

  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
  }
}

export class TabSyncUnavailableError extends TabSyncError {
  override name = 'TabSyncUnavailableError';
}

export class TabSyncClosedError extends TabSyncError {
  override name = 'TabSyncClosedError';
}

type Handler<Message> = (message: Message) => void;
type Listener<Message> = { type: string | null; handler: Handler<Message> };
type WireMessage = {
  protocol: 1;
  channel: string;
  id: string;
  type: string;
  payload: unknown;
  sender: string;
  sentAt: number;
};

export class TabSync<Messages extends MessageMap> {
  readonly instanceId: string;
  private readonly transport: MessageTransport;
  private readonly now: () => number;
  private readonly listeners = new Set<Listener<SyncMessage<Messages>>>();
  private readonly onMessage = (event: { data: unknown }): void => {
    this.receive(event.data);
  };
  private closed = false;

  constructor(
    readonly channelName: string,
    options: TabSyncOptions = {},
  ) {
    if (!channelName.trim()) throw new TypeError('Channel name must not be empty.');
    this.instanceId = options.instanceId ?? createInstanceId();
    if (!this.instanceId.trim()) throw new TypeError('Instance ID must not be empty.');
    this.now = options.now ?? Date.now;
    const factory = options.transportFactory ?? defaultTransportFactory;
    this.transport = factory(channelName);
    this.transport.addEventListener('message', this.onMessage);
  }

  send<Type extends Extract<keyof Messages, string>>(type: Type, payload: Messages[Type]): string {
    this.assertOpen();
    if (!type) throw new TypeError('Message type must not be empty.');
    const id = createMessageId();
    const envelope: WireMessage = {
      protocol: 1,
      channel: this.channelName,
      id,
      type,
      payload,
      sender: this.instanceId,
      sentAt: this.now(),
    };
    this.transport.postMessage(envelope);
    return id;
  }

  subscribe<Type extends Extract<keyof Messages, string>>(
    type: Type,
    handler: (message: Extract<SyncMessage<Messages>, { type: Type }>) => void,
  ): () => void {
    this.assertOpen();
    if (!type) throw new TypeError('Message type must not be empty.');
    const listener: Listener<SyncMessage<Messages>> = {
      type,
      handler: handler as Handler<SyncMessage<Messages>>,
    };
    return this.addListener(listener);
  }

  subscribeAll(handler: (message: SyncMessage<Messages>) => void): () => void {
    this.assertOpen();
    return this.addListener({ type: null, handler });
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.transport.removeEventListener('message', this.onMessage);
    try {
      this.transport.close();
    } finally {
      this.listeners.clear();
    }
  }

  private addListener(listener: Listener<SyncMessage<Messages>>): () => void {
    this.listeners.add(listener);
    let active = true;
    return () => {
      if (!active) return;
      active = false;
      this.listeners.delete(listener);
    };
  }

  private receive(value: unknown): void {
    if (
      !isWireMessage(value) ||
      value.protocol !== 1 ||
      value.channel !== this.channelName ||
      value.sender === this.instanceId ||
      !value.type
    ) {
      return;
    }
    const message = {
      id: value.id,
      type: value.type,
      payload: value.payload,
      sender: value.sender,
      sentAt: value.sentAt,
    } as SyncMessage<Messages>;
    for (const listener of [...this.listeners]) {
      if (listener.type === null || listener.type === message.type) {
        listener.handler(message);
      }
    }
  }

  private assertOpen(): void {
    if (this.closed) throw new TabSyncClosedError('This tab sync instance is closed.');
  }
}

export function createTabSync<Messages extends MessageMap>(
  channelName: string,
  options?: TabSyncOptions,
): TabSync<Messages> {
  return new TabSync<Messages>(channelName, options);
}

function defaultTransportFactory(channelName: string): MessageTransport {
  if (typeof globalThis.BroadcastChannel !== 'function') {
    throw new TabSyncUnavailableError(
      'BroadcastChannel is unavailable; provide a transportFactory.',
    );
  }
  return new BroadcastChannel(channelName) as unknown as MessageTransport;
}

function isWireMessage(value: unknown): value is WireMessage {
  if (typeof value !== 'object' || value === null) return false;
  const record = value as Record<string, unknown>;
  return (
    record.protocol === 1 &&
    typeof record.channel === 'string' &&
    typeof record.id === 'string' &&
    typeof record.type === 'string' &&
    Object.hasOwn(record, 'payload') &&
    typeof record.sender === 'string' &&
    typeof record.sentAt === 'number' &&
    Number.isFinite(record.sentAt)
  );
}

let fallbackCounter = 0;

function createInstanceId(): string {
  const randomUUID = globalThis.crypto?.randomUUID;
  if (randomUUID) return randomUUID.call(globalThis.crypto);
  fallbackCounter += 1;
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}-${fallbackCounter}`;
}

function createMessageId(): string {
  const randomUUID = globalThis.crypto?.randomUUID;
  if (randomUUID) return randomUUID.call(globalThis.crypto);
  fallbackCounter += 1;
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}-${fallbackCounter}`;
}
