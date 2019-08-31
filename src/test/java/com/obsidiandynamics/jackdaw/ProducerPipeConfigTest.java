package com.obsidiandynamics.jackdaw;

import static org.junit.Assert.*;

import java.io.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.verifier.*;
import com.obsidiandynamics.yconf.*;

public final class ProducerPipeConfigTest {
  @Test
  public void testPojo() {
    PojoVerifier.forClass(ProducerPipeConfig.class).verify();
  }
  
  @Test
  public void testFluent() {
    FluentVerifier.forClass(ProducerPipeConfig.class).verify();
  }
  
  @Test
  public void testConfig() throws IOException {
    final ProducerPipeConfig config = new MappingContext()
        .withParser(new SnakeyamlParser())
        .fromStream(ProducerPipeConfigTest.class.getClassLoader().getResourceAsStream("producerpipe.conf"))
        .map(ProducerPipeConfig.class);
    config.validate();
    
    assertEquals(true, config.isAsync());
    Assertions.assertToStringOverride(config);
  }
  
  @Test
  public void testValidate_default() {
    new ProducerPipeConfig().validate();
  }
}
