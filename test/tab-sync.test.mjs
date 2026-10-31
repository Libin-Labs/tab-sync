import test from 'node:test';
import assert from 'node:assert/strict';
import { createTabSync, TabSyncClosedError } from '../dist/esm/index.js';

class Hub {
  endpoints = new Map();

  create(name) {
    let channels = this.endpoints.get(name);
    if (!channels) this.endpoints.set(name, (channels = new Set()));
    const listeners = new Set();
    const endpoint = {
      closed: false,
      postMessage: (data) => {
        structuredClone(data);
        for (const other of channels) {
          if (other !== endpoint && !other.closed) {
            queueMicrotask(() => {
              for (const listener of other.listeners) listener({ data: structuredClone(data) });
            });
          }
        }
      },
      addEventListener: (_type, listener) => listeners.add(listener),
      removeEventListener: (_type, listener) => listeners.delete(listener),
      close: () => {
        endpoint.closed = true;
        channels.delete(endpoint);
      },
      listeners,
    };
    channels.add(endpoint);
    return endpoint;
  }
}

test('delivers typed channel messages between instances and filters subscriptions', async () => {
  const hub = new Hub();
  const first = createTabSync('orders', {
    instanceId: 'tab-a',
    now: () => 123,
    transportFactory: (name) => hub.create(name),
  });
  const second = createTabSync('orders', {
    instanceId: 'tab-b',
    now: () => 456,
    transportFactory: (name) => hub.create(name),
  });
  const received = [];
  second.subscribe('changed', (message) => received.push(message));
  second.subscribe('other', () => assert.fail('wrong message type delivered'));

  const id = first.send('changed', { orderId: 7, status: 'ready' });
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(received, [
    {
      id,
      type: 'changed',
      payload: { orderId: 7, status: 'ready' },
      sender: 'tab-a',
      sentAt: 123,
    },
  ]);

  first.close();
  second.close();
});

test('unsubscribes, ignores its own sender, and closes transport exactly once', async () => {
  const hub = new Hub();
  const sync = createTabSync('cleanup', {
    instanceId: 'same',
    transportFactory: (name) => hub.create(name),
  });
  const transport = hub.endpoints.get('cleanup').values().next().value;
  let calls = 0;
  const unsubscribe = sync.subscribeAll(() => calls++);
  unsubscribe();
  unsubscribe();
  transport.listeners.forEach((listener) =>
    listener({
      data: {
        protocol: 1,
        channel: 'cleanup',
        id: 'm1',
        type: 'changed',
        payload: null,
        sender: 'same',
        sentAt: 1,
      },
    }),
  );
  sync.close();
  sync.close();
  assert.equal(calls, 0);
  assert.equal(transport.closed, true);
  assert.throws(() => sync.send('changed', null), TabSyncClosedError);
  assert.throws(() => sync.subscribeAll(() => {}), TabSyncClosedError);
});

test('validates malformed envelopes without invoking subscribers', () => {
  const hub = new Hub();
  const sync = createTabSync('validation', {
    instanceId: 'receiver',
    transportFactory: (name) => hub.create(name),
  });
  let calls = 0;
  sync.subscribeAll(() => calls++);
  const endpoint = hub.endpoints.get('validation').values().next().value;
  for (const data of [
    null,
    { protocol: 1, channel: 'wrong', id: 'x', type: 'ok', payload: null, sender: 'a', sentAt: 1 },
    { protocol: 1, channel: 'validation', id: 'x', type: 'ok', sender: 'a', sentAt: 1 },
  ]) {
    for (const listener of endpoint.listeners) listener({ data });
  }
  assert.equal(calls, 0);
  sync.close();
});
