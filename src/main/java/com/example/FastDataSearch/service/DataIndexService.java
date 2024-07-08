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
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class DataIndexService {

    @Autowired
    private ElasticsearchOperations elasticsearchOperations;

    private static final int CHUNK_SIZE = 10000;

    public String ingestData(MultipartFile file) {
        String indexName = "idx_" + System.currentTimeMillis();

        try {
            IndexOperations indexOps = elasticsearchOperations.indexOps(IndexCoordinates.of(indexName));
            indexOps.create();

            System.out.println("LOG S1: Created index: " + indexName);

            boolean hasMoreData = true;

            while (hasMoreData) {
                List<Map<String, String>> data = CSVUtils.parseCSV(file, CHUNK_SIZE);
                if (data.isEmpty()) {
                    hasMoreData = false;
                    continue;
                }

                System.out.println("LOG S2: Parsed " + data.size() + " records from CSV.");

                List<IndexQuery> indexQueries = data.stream()
                        .map(record -> new IndexQueryBuilder().withObject(record).build())
                        .collect(Collectors.toList());

                elasticsearchOperations.bulkIndex(indexQueries, IndexCoordinates.of(indexName));
                System.out.println("LOG S3: Bulk indexing completed for index: " + indexName);

                if (data.size() < CHUNK_SIZE) {
                    hasMoreData = false;
                }
            }
        } catch (Exception e) {
            System.err.println("Error during data ingestion: " + e.getMessage());
            return "Error during data ingestion.";
        }

        return indexName;
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

    public List<Map<String, Object>> queryDataset(String indexName, Map<String, String> queryParameters) {
        try {
            Criteria criteria = queryParameters.entrySet()
                    .stream()
                    .map(entry -> Criteria.where(entry.getKey()).is(entry.getValue()))
                    .reduce(Criteria::and)
                    .orElse(new Criteria());

            CriteriaQuery query = new CriteriaQuery(criteria);
            System.out.println("LOG S8: Constructing query for index: " + indexName + " with criteria: " + criteria);

            List<Map<String, Object>> results = elasticsearchOperations.search(query, Map.class, IndexCoordinates.of(indexName))
                    .stream()
                    .map(searchHit -> (Map<String, Object>) searchHit.getContent())
                    .collect(Collectors.toList());

            System.out.println("LOG S9: Query executed successfully. Results count: " + results.size());
            return results;
        } catch (Exception e) {
            System.err.println("Error executing query: " + e.getMessage());
            return new ArrayList<>();
        }
    }
}
