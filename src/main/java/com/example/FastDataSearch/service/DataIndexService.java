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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class DataIndexService {

    @Autowired
    private ElasticsearchOperations elasticsearchOperations;

    // streaming approach to process the CSV data in chunks rather than loading the entire file into memory at once
    private static final int CHUNK_SIZE = 10000; // Adjust chunk size as needed

    public String ingestData(MultipartFile file) {
        String indexName = "idx_" + System.currentTimeMillis();

        IndexOperations indexOps = elasticsearchOperations.indexOps(IndexCoordinates.of(indexName));
        indexOps.create();
        // indexOps.delete();
        // boolean exists = indexOps.exists();
        // Document mapping = indexOps.getMapping(); // get the mapping of the index

        System.out.println("LOG 2: Created index: " + indexName);

        boolean hasMoreData = true;

        while (hasMoreData) {
            List<Map<String, String>> data = CSVUtils.parseCSV(file, CHUNK_SIZE);
            if (data.isEmpty()) {
                hasMoreData = false;
                continue;
            }

            System.out.println("LOG 1: Parsed " + data.size() + " records from CSV.");

            List<IndexQuery> indexQueries = data.stream()
                    .map(record -> new IndexQueryBuilder().withObject(record).build())
                    .collect(Collectors.toList());

            elasticsearchOperations.bulkIndex(indexQueries, IndexCoordinates.of(indexName));
            System.out.println("LOG 3: Bulk indexing completed for index: " + indexName);

            if (data.size() < CHUNK_SIZE) {
                hasMoreData = false;
            }
        }

        return indexName;
    }

    public List<String> listDatasets() {
        IndexOperations indexOps = elasticsearchOperations.indexOps(IndexCoordinates.of("")); // Initializing IndexOperations with an empty index coordinate to target the entire cluster.
        Map<String, Set<AliasData>> aliases = indexOps.getAliases(); // <alias, index names>

        System.out.println("LOG 4: Aliases retrieved: " + aliases.keySet());

        return new ArrayList<>(aliases.keySet());
    }


    public List<String> getColumns(String indexName) {
        // Using mapping to get columns
        IndexOperations indexOps = elasticsearchOperations.indexOps(IndexCoordinates.of(indexName));
        Document mapping = (Document) indexOps.getMapping(); // doc of specific mapping

        System.out.println("LOG 5: Retrieving mapping for index: " + indexName);

        if (!mapping.isEmpty()) {
            Map<String, Object> properties = (Map<String, Object>) mapping.get("properties"); // properties -> map of {column name, field properties (name, age, etc with types) }

            System.out.println("LOG 6: Properties retrieved: " + properties.keySet());

            return new ArrayList<>(properties.keySet());
        }

        System.out.println("LOG 7: No properties found for index: " + indexName);
        return List.of();
    }

    public List<Map<String, Object>> queryDataset(String indexName, Map<String, String> queryParameters) {
        // Build Criteria
        Criteria criteria = queryParameters.entrySet().stream()
                .map(entry -> Criteria.where(entry.getKey()).is(entry.getValue()))
                .reduce(Criteria::and)
                .orElse(new Criteria());

        CriteriaQuery query = new CriteriaQuery(criteria);
        System.out.println("LOG 8: Constructing query for index: " + indexName + " with criteria: " + criteria);

        List<Map<String, Object>> results = elasticsearchOperations.search(query, Map.class, IndexCoordinates.of(indexName))
                .stream()
                .map(searchHit -> (Map<String, Object>) searchHit.getContent())
                .collect(Collectors.toList());

        System.out.println("LOG 9: Query executed successfully. Results count: " + results.size());
        return results;
    }
}
