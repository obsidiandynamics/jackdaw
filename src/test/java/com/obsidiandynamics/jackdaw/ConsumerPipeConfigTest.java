package com.obsidiandynamics.jackdaw;

import static org.junit.Assert.*;

import java.io.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.verifier.*;
import com.obsidiandynamics.yconf.*;

public final class ConsumerPipeConfigTest {
  @Test
  public void testPojo() {
    PojoVerifier.forClass(ConsumerPipeConfig.class).verify();
  }
  
  @Test
  public void testFluent() {
    FluentVerifier.forClass(ConsumerPipeConfig.class).verify();
  }
  
  @Test
  public void testConfigFile() throws IOException {
    final ConsumerPipeConfig config = new MappingContext()
        .withParser(new SnakeyamlParser())
        .fromStream(ConsumerPipeConfigTest.class.getClassLoader().getResourceAsStream("consumerpipe.conf"))
        .map(ConsumerPipeConfig.class);
    config.validate();

    assertFalse(config.isAsync());
    assertEquals(16, config.getBacklogBatches());
    Assertions.assertToStringOverride(config);
  }
  
  @Test
  public void testConfigObject() {
    final ConsumerPipeConfig config = new ConsumerPipeConfig()
        .withAsync(true)
        .withBacklogBatches(10);
    config.validate();
    
    assertTrue(config.isAsync());
    assertEquals(10, config.getBacklogBatches());
  }
  
  @Test
  public void testValidate_default() {
    new ConsumerPipeConfig().validate();
  }
}
