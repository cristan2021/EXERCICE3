package com.kafka;

import java.util.Scanner;

public class ConsMain {
    public static void main(String[] args) {
        // Scanner scanner = new Scanner(System.in);
        // System.out.println("Veuillez saisir le groupId pour le consommateur :");
        // String groupId = scanner.nextLine();
        Consommateur cons = new Consommateur();
        cons.run();

    }
}
