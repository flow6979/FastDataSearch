package com.example.FastDataSearch.service;

import com.example.FastDataSearch.util.CSVUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.index.AliasData;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.stereotype.Service;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.PageImpl;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class DataIndexService {

    @Autowired
    private ElasticsearchOperations elasticsearchOperations;

    public String ingestData(MultipartFile file) {
        String indexName = "idx_" + System.currentTimeMillis();

        try {
            IndexOperations indexOps = elasticsearchOperations.indexOps(IndexCoordinates.of(indexName));
            indexOps.create();

            System.out.println("LOG S1: Created index: " + indexName);

                List<Map<String, String>> data = CSVUtils.parseCSV(file);

                System.out.println("LOG S2: Parsed " + data.size() + " records from CSV.");

                List<IndexQuery> indexQueries = data.stream()
                        .map(record -> new IndexQueryBuilder().withObject(record).build())
                        .collect(Collectors.toList());

                elasticsearchOperations.bulkIndex(indexQueries, IndexCoordinates.of(indexName));
                System.out.println("LOG S3: Bulk indexing completed for index: " + indexName);

        } catch (Exception e) {
            System.err.println("Error during data ingestion: " + e.getMessage());
            return "Error during data ingestion.";
        }

        return indexName;
    }

    public long getDatasetSize(String indexName) {
        try {
            CriteriaQuery query = new CriteriaQuery(new Criteria());
            return elasticsearchOperations.count(query, IndexCoordinates.of(indexName));
        } catch (Exception e) {
            System.err.println("Error retrieving dataset size: " + e.getMessage());
            return 0;
        }
    }

    public List<String> listDatasets() {
        try {
            IndexOperations indexOps = elasticsearchOperations.indexOps(IndexCoordinates.of(""));
            Map<String, Set<AliasData>> aliases = indexOps.getAliases();

            System.out.println("LOG S4: Aliases retrieved: " + aliases.keySet());

            return new ArrayList<>(aliases.keySet());
        } catch (Exception e) {
            System.err.println("Error retrieving datasets: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    public List<String> getColumns(String indexName) {
        try {
            IndexOperations indexOps = elasticsearchOperations.indexOps(IndexCoordinates.of(indexName));
            Document mapping = (Document) indexOps.getMapping();

            System.out.println("LOG S5: Retrieving mapping for index: " + indexName);

            if (!mapping.isEmpty()) {
                Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");

                System.out.println("LOG S6: Properties retrieved: " + properties.keySet());

                return new ArrayList<>(properties.keySet());
            }

            System.out.println("LOG S7: No properties found for index: " + indexName);
            return List.of();
        } catch (Exception e) {
            System.err.println("Error retrieving columns: " + e.getMessage());
            return List.of();
        }
    }

    public Map<String, Object> queryDataset(String indexName, Map<String, List<String>> queryParameters,int page, int size) {
        // Build Criteria
        Criteria criteria = queryParameters.entrySet().stream()
                .filter(entry -> !entry.getKey().equalsIgnoreCase("page") && !entry.getKey().equalsIgnoreCase("size"))
                .map(entry -> Criteria.where(entry.getKey()).in(entry.getValue()))
                .reduce(Criteria::and)
                .orElse(new Criteria());

        Pageable pageable = PageRequest.of(page, size);
        CriteriaQuery query = new CriteriaQuery(criteria).setPageable(pageable);
        System.out.println("LOG 8: Constructing query for index: " + indexName + " with criteria: " + criteria);
        System.out.println("LOG 8: Page: " + page + ", Size: " + size);

        // Execute the search query with pagination
        SearchHits<Map> searchHits = elasticsearchOperations.search(query, Map.class, IndexCoordinates.of(indexName));
        System.out.println("LOG 9: SearchHits: " + searchHits.getTotalHits() + " total hits");

        List<Map<String, Object>> results = searchHits.getSearchHits()
                .stream()
                .map(hit -> (Map<String, Object>) hit.getContent())
                .collect(Collectors.toList());

        long totalHits = searchHits.getTotalHits();
        Page<Map<String, Object>> resultsPage = new PageImpl<>(results, pageable, totalHits);

        System.out.println("LOG 10: Query executed successfully. Results count: " + results.size());

        //return results;

        return Map.of(
                "results", results,
                "currentPage", resultsPage.getNumber(),
                "totalItems", resultsPage.getTotalElements(),
                "totalPages", resultsPage.getTotalPages()
        );
    }
}
