import { createTabSync } from '@libinprasanth/tab-sync';

type Messages = { changed: { count: number } };
const sync = createTabSync<Messages>('types');
sync.send('changed', { count: 1 });
sync.subscribe('changed', ({ payload }) => {
  const count: number = payload.count;
  void count;
});
// @ts-expect-error The changed message requires a numeric count.
sync.send('changed', { count: 'wrong' });
sync.close();
