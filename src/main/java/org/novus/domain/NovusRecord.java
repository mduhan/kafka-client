package org.novus.domain;

/**
 * 
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 * @param <K>
 * @param <V>
 */
public class NovusRecord<K, V> {

  private K key;

  private V value;

  public NovusRecord(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

}
