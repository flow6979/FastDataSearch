package com.example.FastDataSearch.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CSVUtils {

    private static final Logger logger = LoggerFactory.getLogger(CSVUtils.class);

    public static List<Map<String, String>> parseCSV(MultipartFile file, int chunkSize) {
        List<Map<String, String>> data = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            String headerLine = br.readLine();

            if (headerLine != null) {
                String[] headers = headerLine.split(",");
                String line;
                int count = 0;

                while ((line = br.readLine()) != null) {
                    String[] values = line.split(",");
                    Map<String, String> row = new HashMap<>();

                    for (int i = 0; i < headers.length; i++) {
                        row.put(headers[i], values[i]);
                    }
                    data.add(row);
                    count++;

                    if (count >= chunkSize) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error parsing CSV file", e);
            throw new RuntimeException("Failed to parse CSV file", e);
        }
        return data;
    }
}
