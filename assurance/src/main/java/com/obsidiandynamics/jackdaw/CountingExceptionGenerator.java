package com.obsidiandynamics.jackdaw;

import static com.obsidiandynamics.func.Functions.*;

final class CountingExceptionGenerator<C, X extends Throwable> implements ExceptionGenerator<C, X> {
  private final X exception;
  private final int times;
  private int thrown;
  
  CountingExceptionGenerator(X exception, int times) {
    this.exception = mustExist(exception, "Exception cannot be null");
    this.times = mustBeGreaterOrEqual(times, 0, illegalArgument("Number of times cannot be negative"));
  }
  
  @Override 
  public X inspect(C obj) {
    if (thrown < times) {
      try {
        return exception;
      } finally {
        thrown++;
      }
    } else {
      return null;
    }
  }
}
