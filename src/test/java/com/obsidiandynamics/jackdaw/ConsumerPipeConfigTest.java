package com.obsidiandynamics.jackdaw;

import static org.junit.Assert.*;

import java.io.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.yconf.*;

public final class ConsumerPipeConfigTest {
  @Test
  public void testConfig() throws IOException {
    final ConsumerPipeConfig config = new MappingContext()
        .withParser(new SnakeyamlParser())
        .fromStream(ConsumerPipeConfigTest.class.getClassLoader().getResourceAsStream("consumerpipe.conf"))
        .map(ConsumerPipeConfig.class);
    
    assertEquals(false, config.isAsync());
    assertEquals(16, config.getBacklogBatches());
    Assertions.assertToStringOverride(config);
  }
}
