package com.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Producteur implements Runnable {
    private long id;
    private final List<String> batimentS = Arrays.asList("BatimentA", "BatimentB", "BatimentC");
    private final List<String> salleS = Arrays.asList("Salle101", "Salle102", "Salle103");

    public Producteur(long id) {
        this.id = id;
    }

    public void run() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, Double> producteur = new KafkaProducer<>(props);
        Random random = new Random();

        while (true) {
            for (String batiment : batimentS) {
                for (String salle : salleS) {
                    String message = "prod" + id + " " + batiment + "-" + salle + "-"
                            + String.format("%.2f", 15 + (random.nextDouble() * 10));

                    try {
                        producteur.send(new ProducerRecord<>("topic1", batiment, message));
                        System.out.println("envoyé: " + message);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }

        producteur.close();
        System.out.println("Producteur terminé.");
    }
}
