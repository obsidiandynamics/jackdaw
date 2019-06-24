package com.obsidiandynamics.jackdaw;

public final class ProducerException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public ProducerException(String m, RuntimeException cause) {
    super(m, cause);
  }
}
