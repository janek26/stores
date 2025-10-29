import { createHydrationCoordinator, HydrationCoordinator } from 'src/utils/hydrationCoordinator';
import { StateCreator, SetStateArgs, SetState } from '../types';
import { ensureError } from 'src/logger';
import { isPromiseLike } from '../utils/promiseUtils';

type WrappedOnRehydrateStorage<S> = (
  userCallback?: (state: S) => ((finalState?: S, error?: unknown) => void) | void,
  preFlushCallback?: () => void,
  postFlushCallback?: () => void
) => (state: S) => (finalState?: S, error?: unknown) => void;

/**
 * Creates hydration-gating middleware that queues state updates until rehydration completes.
 *
 * **Before hydration**: Queues all `set()` calls and flushes them sequentially after rehydration.
 *
 * **After hydration**: Passes `set()` calls through synchronously without batching.
 *
 * This ensures persistence sees a fully rehydrated base state before applying queued updates,
 * while keeping in-memory state changes synchronous post-hydration.
 */
export function createHydrationGate<S>(stateCreator: StateCreator<S>): {
  hydrationPromise: () => Promise<void>;
  stateCreator: StateCreator<S>;
  wrapOnRehydrateStorage: WrappedOnRehydrateStorage<S>;
} {
  let coordinator: HydrationCoordinator | undefined = undefined;
  let isHydrated = false;
  let resolvedPromise: Promise<void> | undefined = undefined;
  let flushPendingSetCalls: (() => void) | undefined = undefined;

  const wrappedStateCreator: StateCreator<S> = (set, get, api) => {
    let pendingSetCalls: Array<SetStateArgs<S>> | undefined = undefined;
    const originalSet = set;

    const deferredSet: SetState<S> = (...args) => {
      // Always apply updates immediately to state
      const result = args[1] === true ? set(args[0], args[1]) : set(args[0]);

      // -- Hydrating: defer persistence to be flushed after hydration
      if (!isHydrated) {
        if (!coordinator) coordinator = createHydrationCoordinator();
        if (!pendingSetCalls) pendingSetCalls = [];
        pendingSetCalls.push(args);
      }

      return result;
    };

    const result = stateCreator(deferredSet, get, api);

    flushPendingSetCalls = async () => {
      if (!pendingSetCalls?.length) return;
      // Updates were already applied to state, just persist them by calling originalSet
      for (const args of pendingSetCalls) {
        const result = args[1] ? originalSet(args[0], args[1]) : originalSet(args[0]);
        if (isPromiseLike(result)) {
          await result;
        }
      }
      pendingSetCalls = undefined;
    };

    return result;
  };

  const hydrationPromise: () => Promise<void> = () => {
    if (isHydrated) return coordinator?.promise ?? (resolvedPromise ??= Promise.resolve());
    if (!coordinator) coordinator = createHydrationCoordinator();
    return coordinator.promise;
  };

  /**
   * Wraps `onRehydrateStorage` to coordinate the hydration lifecycle.
   */
  const wrapOnRehydrateStorage: WrappedOnRehydrateStorage<S> = (userCallback, preFlushCallback, postFlushCallback) => {
    return state => {
      const userRehydrateCallback = userCallback?.(state);
      return async (finalState, error) => {
        isHydrated = true;

        // This allows the sync enhancer to flush before processing queued set() calls.
        if (preFlushCallback) preFlushCallback();
        if (flushPendingSetCalls) await flushPendingSetCalls();
        if (postFlushCallback) postFlushCallback();

        if (error) coordinator?.fail(ensureError(error));
        else coordinator?.complete();
        userRehydrateCallback?.(finalState, error);
      };
    };
  };

  return {
    hydrationPromise,
    stateCreator: wrappedStateCreator,
    wrapOnRehydrateStorage,
  };
}
