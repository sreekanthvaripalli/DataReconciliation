package org.sree;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataReconciliation {

    /*
        Define mappings between columns of two files(file1.txt and file2.txt case-sensitive)
     */
    static Map<String, String> mapOfColumnsInFiles = Map.of(
        "LastName", "Last_Name",
            "FirstName", "First_Name",
            "Country", "Origin_Country", // Example: different names for same data
            "ID", "ID"
            );

    /*
        Define primary identifier(column) to match in both files(can take multiple).
     */
    static List<String> primaryIdentifier = List.of("ID");

    public static void main(String[] args) throws IOException {
        String file1 = "src/main/resources/file1.txt";
        String file2 = "src/main/resources/file2.txt";
        String file3 = "src/main/resources/file3.txt";

        List<Map<String, String>> data1 = readPipeSeparatedFileAsListOfMap(file1);
        List<Map<String, String>> data2 = readPipeSeparatedFileAsListOfMap(file2);
        List<Map<String, String>> data3 = readPipeSeparatedFileAsListOfMap(file3);

        Map<String, Map<String, String>> map1 = buildLookupMapByPrimaryIdentifier(data1, primaryIdentifier);
        Map<String, Map<String, String>> map2 = buildLookupMapByPrimaryIdentifier(data2, primaryIdentifier);
        Map<String, Map<String, String>> map3 = buildLookupMapByPrimaryIdentifier(data3, primaryIdentifier);

        // Reconciliation through hashing
        Set<String> allKeys = new HashSet<>();
        allKeys.addAll(map1.keySet());
        allKeys.addAll(map2.keySet());
        //comment above line and uncomment below line to create keys for file3.txt which matches completely
        //allKeys.addAll(map3.keySet());

        for (String key : allKeys) {
            Map<String, String> rec1 = map1.get(key);
            Map<String, String> rec2 = map2.get(key);
            //comment above line and uncomment below line to test file which matches completely
            //Map<String, String> rec2 = map3.get(key);

            if (rec1 == null) {
                System.out.println("Key missing in File1: " + key);
            } else if (rec2 == null) {
                System.out.println("Key missing in File2: " + key);
            } else {
                // Compare common columns
                for (String col1 : mapOfColumnsInFiles.keySet()) {
                    String col2 = mapOfColumnsInFiles.get(col1);
                    String val1 = rec1.getOrDefault(col1, "");
                    String val2 = rec2.getOrDefault(col2, "");
                    if (!val1.equals(val2)) {
                        System.out.printf("Key mismatched for %s: %s (File1: %s, File2: %s)%n", key, col1, val1, val2);
                    }
                }
            }
        }
    }

    static List<Map<String, String>> readPipeSeparatedFileAsListOfMap(String filename) throws IOException {
        List<Map<String, String>> data = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String headerLine = br.readLine();
            if (headerLine == null) return data;
            String[] headers = headerLine.split("\\|");
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split("\\|", -1);
                Map<String, String> row = new HashMap<>();
                for (int i = 0; i < headers.length && i < values.length; i++) {
                    row.put(headers[i], values[i]);
                }
                data.add(row);
            }
        }
        return data;
    }

    static Map<String, Map<String, String>> buildLookupMapByPrimaryIdentifier(List<Map<String, String>> data, List<String> keyCols) {
        Map<String, Map<String, String>> map = new HashMap<>();
        for (Map<String, String> row : data) {
            StringBuilder key = new StringBuilder();
            for (String col : keyCols) {
                key.append(row.getOrDefault(col, "")).append("|");
            }
            map.put(key.toString(), row);
        }
        return map;
    }
}
