package com.obsidiandynamics.jackdaw;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

public final class KeyedBlockingQueue<K, E> {
  private final ConcurrentHashMap<K, BlockingQueue<E>> map = new ConcurrentHashMap<>();
  
  private final Supplier<BlockingQueue<E>> partitionFactory;
  
  public KeyedBlockingQueue(Supplier<BlockingQueue<E>> partitionFactory) {
    this.partitionFactory = partitionFactory;
  }
  
  public BlockingQueue<E> forKey(K key) {
    return map.computeIfAbsent(key, __key -> partitionFactory.get());
  }
  
  public Map<K, BlockingQueue<E>> map() {
    return map;
  }

  public int totalSize() {
    return map.values().stream().collect(Collectors.summingInt(q -> q.size())).intValue();
  }
}
