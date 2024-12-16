package um.si;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class NarocilaProducer {

    private final static String TOPIC = "kafka-narocila";

    private static KafkaProducer createProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // Configure the KafkaAvroSerializer.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // Schema Registry location.
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        return new KafkaProducer(props);
    }

    private static ProducerRecord<Object, Object> generateRecord(Schema schema) {
        Random rand = new Random();
        GenericRecord avroRecord = new GenericData.Record(schema);

        // Generiranje vrednosti, ki ustrezajo tabeli narocila
        String narociloNo = String.valueOf((int)(Math.random()*(10000 - 0 + 1) + 1));
        long datumProdaje = System.currentTimeMillis(); // Trenutni čas
        double skupniZnesek = 10 + (rand.nextDouble() * 900); // Random znesek med 10 in 1000
        double popust = rand.nextDouble() * 20; // Random popust med 0 in 20
        int dokumentId = rand.nextInt(100) + 1; // Dokument ID med 1 in 100
        int kupecId = rand.nextInt(50) + 1; // Kupec ID med 1 in 50

        // Nastavljanje vrednosti v Avro zapis
        //avroRecord.put("idNarocilo", idNarocilo);
        avroRecord.put("datumProdaje", datumProdaje);
        avroRecord.put("skupniZnesek", skupniZnesek);
        avroRecord.put("popust", popust);
        avroRecord.put("Dokument_idDokument", dokumentId);
        avroRecord.put("Kupec_idKupec", kupecId);

        ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(TOPIC, narociloNo, avroRecord);
        return producerRecord;
    }

    public static void main(String[] args) throws Exception {
        Schema schema = SchemaBuilder.record("Narocilo")
                .fields()
                .requiredLong("datumProdaje")
                .requiredDouble("skupniZnesek")
                .requiredDouble("popust")
                .requiredInt("Dokument_idDokument")
                .requiredInt("Kupec_idKupec")
                .endRecord();

       KafkaProducer producer = createProducer();


        while(true){
            ProducerRecord record = generateRecord(schema);
            producer.send(record);

            System.out.println("[RECORD] Sent new order object.");
            Thread.sleep(1000);
        }
    }

}
