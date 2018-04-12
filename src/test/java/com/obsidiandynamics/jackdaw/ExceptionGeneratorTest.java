package com.obsidiandynamics.jackdaw;

import static org.junit.Assert.*;

import org.junit.*;

public final class ExceptionGeneratorTest {
  @Test
  public void testNever() {
    assertNull(ExceptionGenerator.never().inspect(null));
  }
  
  @Test
  public void testOnce() {
    final Exception cause = new Exception("simulated");
    final ExceptionGenerator<?, ?> gen = ExceptionGenerator.once(cause);
    assertEquals(cause, gen.inspect(null));
    assertNull(gen.inspect(null));
  }

  
  @Test
  public void testTimes() {
    final Exception cause = new Exception("simulated");
    final int times = 3;
    final ExceptionGenerator<?, ?> gen = ExceptionGenerator.times(cause, 3);
    for (int i = 0; i < times; i++) {
      assertEquals(cause, gen.inspect(null));
    }
    assertNull(gen.inspect(null));
  }
}
