# Kafka Client provides simple interfaces to produce and recieve kafka messages (String and avro). This also provides extremely simple way to get distributed lock and store distributed counter.

### Features :-

1. one line code to send kafka string message. Sample code in ProducerTest
2. Simple interface for kafka Avro messages. Sample code in ProducerTest
3. Easy apis to start stream consumer and to get messages from kafka . Sample code in ConsumerTest
4. Distributed lock using zookeeper . Sample code in DistributedLockTest
5. Store long value in zookeeper . Sample code in DistributedCountTest

## Build 
   gradle clean build
   Install in local maven repo by executing below commands from lib directory of kafka client.
   mvn install:install-file -Dfile=kafka-client-1.0.jar -DgroupId=org.novus -DartifactId=kafka-client -Dversion=1.0 -Dpackaging=jar
   
   
## Requisite

   set zookeeperUrl on application start. Defaulted to localhost:2181
   System.setProperty(NovusConstants.ZOOKEEPER_URL, "ZOOKEEPER_URL");

   set schemaRegistryUrl  on application start.Defaulted to http://localhost:8081 
   System.setProperty(NovusConstants.SCHEMA_REGISTRY_URL, "schemaRegistryUrl");   

## Dependencies
repositories {
      
     mavenCentral()
     maven { url "http://packages.confluent.io/maven" }
     maven { url "http://repo.maven.apache.org/maven2" }
}
 
dependencies {
    compile group: 'org.springframework.kafka', name: 'spring-kafka', version:'1.2.2.RELEASE'
    compile group: 'org.apache.avro', name: 'avro', version:'1.8.2'
    compile group: 'io.confluent', name: 'kafka-avro-serializer', version:'3.2.0'
    compile group: 'com.fasterxml.jackson.core', name: 'jackson-databind', version:'2.6.3'
    compile group: 'org.apache.curator', name: 'curator-recipes', version:'2.12.0'
    compile group: 'io.reactivex', name: 'rxjava', version:'1.2.5'
    compile group: 'org.novus', name: 'kafka-client', version:'1.0'
}
   
Important links :-

2. https://opcentral.operative.com/display/ENG/Novus+client+java+Kafka+api+example   java apis producer
3. https://opcentral.operative.com/display/ENG/Novus+client+Java+Consumer+Api+examples  java apis consumer
4. https://opcentral.operative.com/display/ENG/Maintain+distributed+atomic+long+in+zookeeper+with+curator+recipe  atomic long for across cluster
6. Download above jar and install in local by executing below commands from jar location

7. Check Test classes to see novus jar sample working examples
8. Include below dependencies in POM or build.gradle.
   https://opcentral.operative.com/display/ENG/Kafka+Novus+Client+Import+in+project

