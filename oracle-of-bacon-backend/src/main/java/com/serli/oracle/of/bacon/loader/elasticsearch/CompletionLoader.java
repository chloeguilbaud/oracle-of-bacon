package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

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

        String inputFilePath = args[0];
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader
                    .lines()
                    .forEach(line -> {
                        // TODO ElasticSearch insert
                        try {
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
                        System.out.println(line);
                    });
        }

        System.out.println("Inserted total of " + count.get() + " actors");

        client.close();
    }


}
