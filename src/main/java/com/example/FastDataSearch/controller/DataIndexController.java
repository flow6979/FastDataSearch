package com.example.FastDataSearch.controller;

import com.example.FastDataSearch.service.DataIndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/index")
public class DataIndexController {

    @Autowired
    private DataIndexService dataIndexService;

    @PostMapping
    public ResponseEntity<Map<String, String>> ingestData(@RequestParam("file") MultipartFile file) {
        if (file.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("message", "File is empty"));
        }

        System.out.println("LOG C1: Received file: " + file.getOriginalFilename() + ", size: " + file.getSize());

        String indexName = dataIndexService.ingestData(file);
        return ResponseEntity.ok(Map.of("message", "Data ingested successfully", "index", indexName));
    }

    @GetMapping
    public ResponseEntity<Map<String, List<String>>> listDatasets() {
        List<String> datasets = dataIndexService.listDatasets();
        return ResponseEntity.ok(Map.of("datasets", datasets));
    }

    @GetMapping("/{indexName}")
    public ResponseEntity<Map<String, List<String>>> showColumns(@PathVariable String indexName) {
        List<String> columns = dataIndexService.getColumns(indexName);
        return ResponseEntity.ok(Map.of("columns", columns));
    }

    @GetMapping("/{indexName}/search")
    public ResponseEntity<Map<String, List<Map<String, Object>>>> queryDataset(@PathVariable String indexName, @RequestParam Map<String, String> queryParameters) {
        List<Map<String, Object>> results = dataIndexService.queryDataset(indexName, queryParameters);
        return ResponseEntity.ok(Map.of("results", results));
    }
}
