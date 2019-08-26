package com.obsidiandynamics.jackdaw;

/**
 *  Simulates the injection of exceptions into a mocked Kafka client.
 *  
 *  @param <C> Context type.
 *  @param <X> Exception type.
 */
@FunctionalInterface
public interface ExceptionGenerator<C, X extends Throwable> {
  /**
   *  Inspects a given context, and if it is deemed that an error should be simulated, will return
   *  an instance of the appropriate {@link Throwable}. Otherwise, will return {@code null}.
   *  
   *  @param context The context to inspect.
   *  @return An exception, or {@code null}.
   */
  X inspect(C context); // lgtm [java/unused-parameter]
  
  static <C, X extends Throwable> ExceptionGenerator<C, X> never() {
    return __ -> null;
  }
  
  static <C, X extends Throwable> ExceptionGenerator<C, X> once(X exception) {
    return times(exception, 1);
  }
  
  static <C, X extends Throwable> ExceptionGenerator<C, X> times(X exception, int times) {
    return new CountingExceptionGenerator<>(exception, times);
  }
}
