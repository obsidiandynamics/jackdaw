package com.obsidiandynamics.jackdaw;


/**
 *  Simulates the injection of exceptions into a mocked Kafka client.
 *  
 *  @param <C> The context type.
 *  @param <X> The exception type.
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
  X inspect(C context);
  
  static <C, X extends Throwable> ExceptionGenerator<C, X> never() {
    return record -> null;
  }
  
  static <C, X extends Throwable> ExceptionGenerator<C, X> once(X exception) {
    return times(exception, 1);
  }
  
  static <C, X extends Throwable> ExceptionGenerator<C, X> times(X exception, int times) {
    return new ExceptionGenerator<C, X>() {
      private final X ex = exception;
      private int thrown;
      @Override public X inspect(C obj) {
        if (thrown < times) {
          try {
            return ex;
          } finally {
            thrown++;
          }
        } else {
          return null;
        }
      }
    };
  }
}
