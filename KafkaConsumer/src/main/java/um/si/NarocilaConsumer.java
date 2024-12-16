package um.si;


import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import sun.net.www.content.text.Generic;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class NarocilaConsumer {

    private final static String TOPIC = "kafka-narocila";

    private static KafkaConsumer<String, GenericRecord> createConsumer() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "AvroConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-narocila");
        // Configure the KafkaAvroDeserializer.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        // Schema Registry location.
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer(props);
    }

    public static void main(String[] args) throws Exception {
       

        KafkaConsumer consumer = createConsumer()   ;
        consumer.subscribe(Arrays.asList(TOPIC));
        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(2000);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    // Extract the GenericRecord value
                    GenericRecord value = record.value();

                    // Print the offset, key, and each field from the GenericRecord
                    System.out.printf("offset = %d, key = %s, datumProdaje = %d, skupniZnesek = %.2f, popust = %.2f, Dokument_idDokument = %d, Kupec_idKupec = %d\n",
                            record.offset(),
                            record.key(),
                            value.get("datumProdaje"),
                            value.get("skupniZnesek"),
                            value.get("popust"),
                            value.get("Dokument_idDokument"),
                            value.get("Kupec_idKupec"));
                }
            }
        } finally {
            consumer.close();
        }
    }
}
