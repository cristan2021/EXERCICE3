package com.kafka;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import java.time.Duration;

public class traitementTemperature {
    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> temperatureReadings = builder.stream("temperature-sensor");

        TimeWindows tumblingWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)); // Fenêtre fixe de 10 min

        KTable<Windowed<String>, Double> averageTemperatures = temperatureReadings
                .mapValues(value -> Double.parseDouble(value.split(",")[2])) // Extraire la température
                .groupByKey()
                .windowedBy(tumblingWindows)
                .aggregate(
                        () -> new double[] { 0.0, 0 }, // [somme, count]
                        (key, newValue, agg) -> new double[] { agg[0] + newValue, agg[1] + 1 }, // Somme et compteur
                        Materialized.<String, double[], WindowStore>as("average-temperatures-store")
                                .withValueSerde(
                                        Serdes.serdeFrom(new DoubleArraySerializer(), new DoubleArrayDeserializer())))
                .mapValues(agg -> String.format("%.2f", agg[0] / agg[1])); // Calcul de la moyenne et formatage

        // Output les résultats vers un topic
        averageTemperatures.toStream().to("average-temperature-output",
                Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class, 10 * 60 * 1000), Serdes.String()));

    }
}
