package org.novus.cache.rx;

import java.util.Arrays;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.functions.Func3;
import rx.functions.Func4;
import rx.functions.Func5;

/**
 * 
 * This class with register methods for jvm level cache with 1 day timeout
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
public final class RxMemoizer {

  static Timer timer = new Timer();

  private RxMemoizer() {
    // No instances
  }

  /**
   * Return a new version of the function that caches results
   *
   * @param func0
   *          function to wrap
   * @return function caching results
   */
  public static <R> Func0<R> memoize(final Func0<R> func0) {
    return new Func0<R>() {
      private R value;

      @Override
      public R call() {
        if (null == value) {
          synchronized (this) {
            if (null == value) {
              value = func0.call();
            }
          }
        }
        return value;
      }
    };
  }

  /**
   * Return a new version of the function that caches results
   *
   * @param func1
   *          function to wrap
   * @return function caching results
   */
  public static <A, R> Func1<A, R> memoize(final Func1<A, R> func1) {
    final Map<A, R> results = new ConcurrentHashMap<A, R>();
    return new Func1<A, R>() {
      @Override
      public R call(A a) {
        final R cached = results.get(a);
        if (null == cached) {
          final R result = func1.call(a);
          results.put(a, result);
          timeout(a, results);
          return result;
        } else {
          return cached;
        }
      }
    };
  }

  /**
   * Return a new version of the function that caches results
   *
   * @param func2
   *          function to wrap
   * @return function caching results
   */
  public static <A, B, R> Func2<A, B, R> memoize(final Func2<A, B, R> func2) {
    final Map<ArgStorage, R> results = new ConcurrentHashMap<ArgStorage, R>();
    return new Func2<A, B, R>() {
      @Override
      public R call(A a, B b) {
        final ArgStorage args = new ArgStorage(a, b);
        final R cached = results.get(args);
        if (null == cached) {
          final R result = func2.call(a, b);
          results.put(args, result);
          timeout(args, results);
          return result;
        } else {
          return cached;
        }
      }
    };
  }

  /**
   * Return a new version of the function that caches results
   *
   * @param func3
   *          function to wrap
   * @return function caching results
   */
  public static <A, B, C, R> Func3<A, B, C, R> memoize(final Func3<A, B, C, R> func3) {
    final Map<ArgStorage, R> results = new ConcurrentHashMap<ArgStorage, R>();
    return new Func3<A, B, C, R>() {
      @Override
      public R call(A a, B b, C c) {
        final ArgStorage args = new ArgStorage(a, b, c);
        final R cached = results.get(args);
        if (null == cached) {
          final R result = func3.call(a, b, c);
          results.put(args, result);
          timeout(args, results);
          return result;
        } else {
          return cached;
        }
      }
    };
  }

  /**
   * Return a new version of the function that caches results
   *
   * @param func4
   *          function to wrap
   * @return function caching results
   */
  public static <A, B, C, D, R> Func4<A, B, C, D, R> memoize(final Func4<A, B, C, D, R> func4) {
    final Map<ArgStorage, R> results = new ConcurrentHashMap<ArgStorage, R>();
    return new Func4<A, B, C, D, R>() {
      @Override
      public R call(A a, B b, C c, D d) {
        final ArgStorage args = new ArgStorage(a, b, c, d);
        final R cached = results.get(args);
        if (null == cached) {
          final R result = func4.call(a, b, c, d);
          results.put(args, result);
          timeout(args, results);
          return result;
        } else {
          return cached;
        }
      }
    };
  }

  /**
   * Return a new version of the function that caches results
   *
   * @param func5
   *          function to wrap
   * @return function caching results
   */
  public static <A, B, C, D, E, R> Func5<A, B, C, D, E, R> memoize(final Func5<A, B, C, D, E, R> func5) {
    final Map<ArgStorage, R> results = new ConcurrentHashMap<ArgStorage, R>();
    return new Func5<A, B, C, D, E, R>() {
      @Override
      public R call(A a, B b, C c, D d, E e) {
        final ArgStorage args = new ArgStorage(a, b, c, d, e);
        final R cached = results.get(args);
        if (null == cached) {
          final R result = func5.call(a, b, c, d, e);
          results.put(args, result);
          timeout(args, results);
          return result;
        } else {
          return cached;
        }
      }
    };
  }

  private static final class ArgStorage {
    private final Object[] storage;

    private final int hashCode;

    ArgStorage(Object... storage) {
      this.storage = storage;
      this.hashCode = Arrays.hashCode(this.storage);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ArgStorage that = (ArgStorage) o;
      return Arrays.equals(storage, that.storage);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }

  public static <A, R> void timeout(A args, Map<A, R> results) {
    timer.schedule(new TimerTask() {

      @Override
      public void run() {
        results.remove(args);
      }
    }, 86400000);

  }
}