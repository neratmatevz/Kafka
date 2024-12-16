package um.si;


import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.*;
public class OrderStreamer {
    private static Properties setupApp() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "GroupNarocila");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());
        props.put("schema.registry.url", "http://0.0.0.0:8081");
        props.put("input.topic.name", "kafka-narocila");
        props.put("default.deserialization.exception.handler", "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");

        return props;
    }

    public static void main(String[] args) throws Exception {

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                "http://0.0.0.0:8081");

        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, GenericRecord> inputStream = builder.stream("kafka-narocila", Consumed.with(Serdes.String(), valueGenericAvroSerde));

        //prva analiza
        inputStream.map((key, value) -> KeyValue.pair(value.get("Kupec_idKupec").toString(), (Double) value.get("skupniZnesek"))) // Izločimo ID kupca in znesek
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))     //grupiramo po kupec id
                .aggregate(
                        () -> 0.0,
                        (key, value, aggregate) -> aggregate + value, // Aggregator: sešteje vse vrednosti za skupniZnesek po idju
                        Materialized.with(Serdes.String(), Serdes.Double())
                ).toStream()
                .to("kafka-kupec-vsota-zneskov", Produced.with(Serdes.String(), Serdes.Double()));

        inputStream.print(Printed.toSysOut());

        //druga analiza
        KTable<String, Double> vsotaZneskovKupcaStream = builder.table("kafka-kupec-vsota-zneskov", Consumed.with(Serdes.String(), Serdes.Double()));

        vsotaZneskovKupcaStream.filter((key, value) -> Double.valueOf(value) > 500)
                .toStream()
                .to("kafka-filter-500-vsota-zneskov", Produced.with(Serdes.String(), Serdes.Double()));

        vsotaZneskovKupcaStream.toStream().print(Printed.toSysOut());

        //tretja analiza
        inputStream
                .map((k, v) -> new KeyValue<>(v.get("Kupec_idKupec").toString(), 1)) // Izločimo ID kupca in nastavimo vrednost na 1 (za vsako naročilo)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer())) // Grupiramo po ID-ju kupca
                .count() // Štejemo število naročil za vsakega kupca
                .toStream() // Pretvorimo KTable v KStream
                .mapValues(value -> value.toString()) // Pretvorimo število naročil v String
                .to("kafka-kupec-narocila", Produced.with(Serdes.String(), Serdes.String())); // Pošljemo v nov Kafka stream



        Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, setupApp());
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
