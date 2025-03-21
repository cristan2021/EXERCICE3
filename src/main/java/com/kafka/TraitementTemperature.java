package com.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.Materialized;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TraitementTemperature {

        private static final double TEMP_MIN = 15.0;
        private static final double TEMP_MAX = 25.0;

        public void run() {
                Properties proprietes = new Properties();
                proprietes.put(StreamsConfig.APPLICATION_ID_CONFIG, "traitementTemperature");
                proprietes.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                proprietes.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
                proprietes.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
                proprietes.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                                WallclockTimestampExtractor.class.getName());

                StreamsBuilder constructeur = new StreamsBuilder();
                KStream<String, String> flux = constructeur.stream("topic1");

                TimeWindows fenetreTemps = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5));

                KStream<String, Double> fluxTempérature = flux.flatMap((cle, valeur) -> {
                        List<KeyValue<String, Double>> listeCléValeur = new ArrayList<>();
                        try {
                                String[] parties = valeur.split(",");
                                if (parties.length == 3) {
                                        String batiment = parties[0];
                                        String salle = parties[1];
                                        double temperature = Double.parseDouble(parties[2]);
                                        listeCléValeur.add(new KeyValue<>(batiment + "-" + salle, temperature));
                                }
                        } catch (Exception e) {
                                System.err.println("Erreur de traitement des données : " + e.getMessage());
                        }
                        return listeCléValeur;
                });

                KTable<Windowed<String>, KeyValue<Double, Long>> moyenneTempératureParSalle = fluxTempérature
                                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                                .windowedBy(fenetreTemps)
                                .aggregate(

                                                () -> new KeyValue<>(0.0, 0L),
                                                (key, newTemperature, aggValue) -> new KeyValue<>(
                                                                aggValue.key + newTemperature, aggValue.value + 1),
                                                Materialized.with(Serdes.String(),
                                                                new KeyValueSerde<>(Serdes.Double(), Serdes.Long())));

                moyenneTempératureParSalle.toStream()
                                .mapValues((cléFenêtrée, value) -> value.key / value.value)
                                .peek((cléFenêtrée, tempMoyenne) -> System.out.println("Fenêtrage actif - Salle: "
                                                + cléFenêtrée.key() + " | Temp Moyenne: "
                                                + tempMoyenne));

                KStream<String, String> alertes = moyenneTempératureParSalle.toStream()
                                .filter((cléFenêtrée, tempMoyenne) -> tempMoyenne < TEMP_MIN || tempMoyenne > TEMP_MAX)
                                .map((cléFenêtrée, tempMoyenne) -> {
                                        String salle = cléFenêtrée.key();
                                        String messageAlerte = "Alerte Temperature : " + salle
                                                        + " | Temp Moyenne: " + tempMoyenne;
                                        System.out.println(messageAlerte);
                                        return new KeyValue<>(salle, messageAlerte);
                                });

                alertes.to("topic2", Produced.with(Serdes.String(), Serdes.String()));

                KafkaStreams streams = new KafkaStreams(constructeur.build(), proprietes);
                streams.start();

                Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close()));

                try {
                        streams.awaitTermination();
                } catch (InterruptedException e) {
                        streams.close();
                }
        }

        public static void main(String[] args) {
                TraitementTemperature traitement = new TraitementTemperature();
                Thread thread = new Thread(() -> traitement.run());
                thread.start();
        }
}
