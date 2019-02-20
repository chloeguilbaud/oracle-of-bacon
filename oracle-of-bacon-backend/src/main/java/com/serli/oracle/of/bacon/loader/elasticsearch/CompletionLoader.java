package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);

    // PENSER A CONFIGURER A FOURNIR UN FICHIER CSV ACTOR EN ENTREE
    public static void main(String[] args) throws IOException, InterruptedException {
        RestHighLevelClient client = ElasticSearchRepository.createClient();

        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }

        PutMappingRequest request = new PutMappingRequest("actors");
        request.type("actor");
        String bodyRequest = "{" +
                                "\"properties\": " +
                                    "{ " +
                                        "\"suggest\": " +
                                            "{ " +
                                                "\"type\": \"completion\"" +
                                            "}," +
                                        "\"name\" : " +
                                            "{" +
                                                "\"type\": \"text\"" +
                                            "}" +
                                    "}" +
                            "}";

        request.source(bodyRequest, XContentType.JSON);

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {

            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                // Rien à faire
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                count.incrementAndGet();
                System.out.println("Inserted total of " + count.get() + " bulk actors");
                //System.out.println("Inserted total of about " + count.get()*5000 + " actors");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.err.println("Fail");
                failure.printStackTrace();
            }

        };

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (bulkRequest, bulkListener) ->
                        client.bulkAsync(bulkRequest, bulkListener);

        BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer, listener);
        builder.setFlushInterval(TimeValue.timeValueSeconds(10));
        builder.setBulkActions(50000);
        builder.setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB));
        builder.setConcurrentRequests(0);

        BulkProcessor bulkProcessor = builder.build();

        String inputFilePath = args[0];
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader
                    .lines()
                    .forEach(line -> {
                        // TODO ElasticSearch insert
                        if (count.get() < 1) {
                            count.incrementAndGet();
                            return;
                        } else {
                            IndexRequest indRequest = new IndexRequest("actors", "actor");
                            Map<String, Object> map = new HashMap<>();
                            String name = line.substring(1, line.length() - 1);
                            map.put("name", name);
                            String[] splitName = name.split("\\s+");
                            map.put("suggest", splitName);
                            indRequest.source(map);
                            bulkProcessor.add(indRequest);
                        }

                        // Insertions sans BulkProcessor
                        /*try {
                            BulkRequest request = new BulkRequest();
                            String nom = "";
                            String prenom = "";
                            if(line.contains(",")) {
                                nom = line.substring(0, line.indexOf(","));
                                prenom = line.substring(line.indexOf(","));
                            }
                            if (count.get() > 2) {
                                request.add(new IndexRequest("actors").id(UUID.randomUUID().toString()).type("actor")
                                                .source(XContentType.JSON,
                                                        "nom", nom, "prenom", prenom)
                                );
                                client.bulk(request);
                            }
                            count.incrementAndGet();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        System.out.println(line);*/
                    });
        }

        client.close();
    }


}
