package com.mycompany.salesorderdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SalesMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text compositeKey = new Text();
    private IntWritable quantity = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(";"); // Adaptez le séparateur ici
        if (!fields[0].equals("Order ID")) { // Ignorer la ligne d'en-tête
            try {
                String product = fields[1]; // Colonne Product
                String address = fields[5]; // Colonne Purchase Address
                String region = extractRegion(address); // Méthode pour extraire la région
                String month = fields[6]; // Colonne Month
                int quantityOrdered = Integer.parseInt(fields[2]); // Colonne Quantity Ordered
                
                compositeKey.set(product + "," + region + "," + month); // Clé composite
                quantity.set(quantityOrdered);
                
                context.write(compositeKey, quantity);
            } catch (Exception e) {
                // Ignorer les lignes mal formatées
            }
        }
    }

    // Méthode pour extraire la région depuis l'adresse
    private String extractRegion(String address) {
        // Supposons que la région soit la partie après le dernier ", "
        String[] parts = address.split(", ");
        return parts[parts.length - 1];
    }
}
