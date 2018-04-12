package com.obsidiandynamics.jackdaw;

@FunctionalInterface
public interface ExceptionGenerator<T, X> {
  X get(T obj);
  
  static <T, X> ExceptionGenerator<T, X> never() {
    return record -> null;
  }
  
  static <T, X> ExceptionGenerator<T, X> once(X exception) {
    return times(exception, 1);
  }
  
  static <T, X> ExceptionGenerator<T, X> times(X exception, int times) {
    return new ExceptionGenerator<T, X>() {
      private final X ex = exception;
      private int thrown;
      @Override public X get(T obj) {
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
