package com.kafka;

public class ProdMain {
    public static void main(String[] args) {
        int numProducteurs = 6;

        Thread[] threads = new Thread[numProducteurs];

        for (int i = 0; i < numProducteurs; i++) {

            Producteur prod = new Producteur(i);
            threads[i] = new Thread(prod);
            threads[i].start();
        }

        for (int i = 0; i < numProducteurs; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Production des messages terminée.");
    }
}
