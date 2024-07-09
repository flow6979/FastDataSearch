package com.example.FastDataSearch.controller;

import com.example.FastDataSearch.service.DataIndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/index")
public class DataIndexController {

    @Autowired
    private DataIndexService dataIndexService;

    @PostMapping
    public ResponseEntity<Map<String, Object>> ingestData(@RequestParam("file") MultipartFile file) {
        if (file.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("message", "File is empty"));
        }

        System.out.println("LOG C1: Received file: " + file.getOriginalFilename() + ", size: " + file.getSize());

        String indexName = dataIndexService.ingestData(file);
        long datasetSize = dataIndexService.getDatasetSize(indexName); // Assuming you have this method in service to get dataset size

        return ResponseEntity.ok(Map.of(
                "message", "Data ingested successfully",
                "index", indexName,
                "dataset size", datasetSize
        ));
    }

    @GetMapping
    public ResponseEntity<Map<String, Object>> listDatasets() {
        List<String> datasets = dataIndexService.listDatasets();
        return ResponseEntity.ok(Map.of(
                "meta data", Map.of("dataset count", datasets.size()),
                "datasets", datasets
        ));
    }

    @GetMapping("/{indexName}")
    public ResponseEntity<Map<String, Object>> showColumns(@PathVariable String indexName) {
        List<String> columns = dataIndexService.getColumns(indexName);
        return ResponseEntity.ok(Map.of(
                "meta data", Map.of(
                        "index", indexName,
                        "Total columns", columns.size()
                ),
                "columns", columns
        ));
    }

    @GetMapping("/{indexName}/search")
    public ResponseEntity<Map<String, Object>> queryDataset(
            @PathVariable String indexName,
            @RequestParam Map<String, String> queryParameters,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        Map<String, List<String>> parsedQueryParameters = queryParameters.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> List.of(e.getValue().split(","))
                ));
        Map<String, Object> results =  dataIndexService.queryDataset(indexName, parsedQueryParameters,page,size);
        return ResponseEntity.ok(results);
    }
}
