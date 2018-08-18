package com.sintecmedia;

import java.util.Arrays;
import java.util.Collection;

import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.novus.common.NovusEnvironment;
import org.novus.kafka.consumer.NovusConsumerListener;

/**
 * *
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
@RunWith(JUnit4.class)
public class ConsumerTest {

  @Test
  public void liveConsumerTest() {

    class SimpleConnector extends NovusConsumerListener<Object, Object> {

      protected SimpleConnector(Collection<String> topics) {
        super(topics);
      }

      @Override
      public void put(ConsumerRecords<Object, Object> comsumerRecords) {
        comsumerRecords.forEach(record -> {
          System.out.println(record.offset());
        });
      }

    }


    NovusEnvironment.DEFAULT.kafkaConsumer("consumerGroupId1",
        new SimpleConnector(Arrays.asList("avroTopic", "avroNestedTopic")));

    // main thread should be up or running to recieve message message in live consumer . That is why while
    // loop
    while (true) {

    }
  }

  @Test
  public void seekToBegginingConsumerTest() {

    ConsumerRecords<Object, Object> records = NovusEnvironment.DEFAULT.kafkaConsumer("consumerGroupI2")
        .seekToBeginning("avroTopic");
    records.forEach(record -> {
      Record struct = (Record) record.value();
      struct.get("module").toString();
      struct.get("timestamp");
    });
  }

  @Test
  public void exactOffsetConsumerTest() {

    ConsumerRecords<Object, Object> records = NovusEnvironment.DEFAULT.kafkaConsumer("consumerGroupI3")
        .seek("avroTopic",
        0, 30); // topic,partition,offset

    records.forEach(record -> {
      Record struct = (Record) record.value();
      struct.get("module").toString();
      struct.get("timestamp");
    });
  }

  @Test
  public void offsetRangeConsumerTest() {

    ConsumerRecords<Object, Object> records = NovusEnvironment.DEFAULT.kafkaConsumer("consumerGroupI4")
        .seek("avroTopic", 0, 0, 3); // topicName,partition,from offset,to offset

    records.forEach(record -> {
      Record struct = (Record) record.value();
      struct.get("module").toString();
      struct.get("timestamp");
    });
  }

}
