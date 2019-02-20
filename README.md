# Oracle of Bacon

## Présentation

L'objectif de l'application est de déterminer le degré de séparation entre Kevin Bacon et un acteur donné via ses rôles dans les films.

Par exemple : **Al Pacino**

* **Al Pacino** a joué dans `Carlito’s Way` avec `Nelson Vasquez`
* `Nelson Vasquez` a joué dans `The Guiding Light` avec **Kevin Bacon**

![](./example-oracle-of-bacon.png)

## Les modules
Le projet est réparti en deux modules :

* [Le back end](./oracle-of-bacon-backend) qui gère la logique et la connexion aux différents Data Stores.
* [Le front end](./oracle-of-bacon-frontend) qui expose l'IHM.

## Pour démarrer
Pour lancer le projet démarrez deux terminaux et positionnez-vous dans ce répertoire :
```BASH
cd <path to directory oracle-of-bacon>
```

Dans le premier, lancez le frontend :
```BASH
cd oracle-of-bacon-frontend
npm install
npm run dev
```

Dans le second, lancez le backend :
```BASH
cd oracle-of-bacon-backend
./gradlew run
```

Rendez-vous sur a page http://localhost:8080.

Pour la suite du projet, nous vous conseillons d'importer le backend dans votre IDE/éditeur préféré. Sauf désir de créativité :smiley:, vous n'avez rien à faire dans le front-end.

## Le data set
Les données sont des données qui proviennent de imdb, le dataset est disponible ici : http://bit.ly/imdbdataset

## Votre mission
Le site a été bouchonné (cf `TODO`), vous devez effectuer les tâches suivantes :
* Importer les données dans Neo4J à l'aide de l'outil d'import : [`ìmport-tool`](http://neo4j.com/docs/operations-manual/current/tutorial/import-tool/).

Solution : 
```bash
bin/neo4j-admin import --nodes:Movie import/imdb-data/movies.csv --nodes:Actor import/imdb-data/actors.csv --relationships import/imdb-data/roles.csv
```

* Implémenter l'Oracle de Bacon à l'aide de Neo4J dans la méthode `com.serli.oracle.of.bacon.repository.Neo4JRepository#getConnectionsToKevinBacon`


Solution : 
```java
    public List<GraphItem> getConnectionsToKevinBacon(String actorName) {
        Session session = driver.session();
        Transaction tx = session.beginTransaction();

        StatementResult result = tx.run(
                "MATCH (bacon:Actor {name: {bacon_name}}), (actor:Actor {name: {actorName}}), path = shortestPath((bacon)-[:PLAYED_IN*]-(actor)) WITH path WHERE length(path) > 1 RETURN path",
                parameters("bacon_name", BACON_NAME, "actorName", actorName)
            );

        return result.list()
                    .stream()
                    .flatMap(record -> record.values().stream().map(Value::asPath))
                    .flatMap(p -> toGraphItems(p).stream())
                    .collect(Collectors.toList());
    }

    private List<GraphItem> toGraphItems(Path path) {
        List<GraphItem> graphItems = toGraphItem(path.nodes(), this::toGraphItem);
        graphItems.addAll(toGraphItem(path.relationships(), this::toGraphItem));

        return graphItems;
    }

    private <T> List<GraphItem> toGraphItem(Iterable<T> iterable, Function<T, GraphItem> toGraphItem) {
        return StreamSupport.stream(iterable.spliterator(), false)
                            .map(toGraphItem)
                            .collect(Collectors.toList());
    }

    private GraphItem toGraphItem(Node n) {
        String type = n.labels().iterator().next();
        String property = type.equals("Actor") ? "name" : "title";

        return new GraphNode(
            n.id(),
            n.get(property).asString(),
            type
        );
    }

    private GraphItem toGraphItem(Relationship relationship) {
        return new GraphEdge(
            relationship.id(),
            relationship.startNodeId(),
            relationship.endNodeId(),
            relationship.type()
        );
    }
```

* Implémenter la gestion du last 10 search à l'aide de Redis dans la méthode `com.serli.oracle.of.bacon.repository.RedisRepository#getLastTenSearches`

Solution : 
```java
    private final Jedis jedis;
    private final String KEY_LAST_SEARCHES = "LAST_SEARCHES";
    private final int MAX_LAST_SEARCHES_NUMBER = 10;

    public RedisRepository() {
        this.jedis = new Jedis("127.0.0.1", 6379);
    }

    public void addToLastTenSearches(String search) {
        this.jedis.lpush(KEY_LAST_SEARCHES, search);
    }

    public List<String> getLastTenSearches() {
        return this.jedis.lrange(KEY_LAST_SEARCHES, 0, MAX_LAST_SEARCHES_NUMBER - 1);
    }
```

* Importer les données à l'aide de ElasticSearch dans `com.serli.oracle.of.bacon.loader.elasticsearch.CompletionLoader` (les liens suivants pourront vous aider : [search](https://www.elastic.co/guide/en/elasticsearch/reference/current/search.html), [mapping](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html) et [suggest](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-suggesters.html))

Solution : 
Le code suivant permet l'insertion des données. Kibana permet la visualisation des insertions. Les données sont correctement enregistrées.
```java
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
```
Dans un deuxième temps nous avons mit en place l'autocompletion. Le code actuellement en place, permet cette fonctionnalités. Toutefois, lorsque nous écrivons dans le champs de saisie, une erreur s'affiche à la place des suggestions. Nous ne sommes pas parvenu à résoudre ce problème. Malgré tout, le degré de séparation entre Kevin Bacon et un acteur donné est fonctionnel.

* Implémenter la suggestion sur le nom des acteurs dans `com.serli.oracle.of.bacon.repository.ElasticSearchRepository#getActorsSuggests`
Voir le problème exposé dans la question précédente.

* Implémenter la recherche des acteurs par nom à l'aide de MongoDB dans `com.serli.oracle.of.bacon.repository.MongoDbRepository#getActorByName`

Solution :
Import des données via le fichier Node `./mongo/index.js` (le fichier .csv n'a volontairement pas été poussé : trop volumineux)
Le code java :
```java
    public Optional<Document> getActorByName(String name) {
        return Optional.ofNullable(this.actorCollection.find(Filters.eq("name", name)).first());
    }
```


## Evaluation

L'évaluation de votre travail sera effectuée selon les critères suivants :
* Bon fonctionnement (First make it work)
* Qualité de la solution implémentée (Then make it good)
* Qualité générale de votre code (et de vos commits, il ne faut pas pousser :smiley:)

La livraison de votre travail s'effectue à l'aide d'une pull-request sur le repository https://github.com/nosql-bootcamp/oracle-of-bacon.

**Vous devez commencer par forker le repository https://github.com/nosql-bootcamp/oracle-of-bacon**

:warning: Si vous êtes sur Windows, il peut y avoir des problèmes au lancement du `frontend`. Si cela se produit, il faut désactiver `eslint`. Pour cela, dans le fichier `oracle-of-bacon-frontend/build/webpack.base.conf.js`, il faut supprimer 
 * les lignes 36 à 49
 ```
 preLoaders: [
      {
        test: /\.vue$/,
        loader: 'eslint',
        include: projectRoot,
        exclude: /node_modules/
      },
      {
        test: /\.js$/,
        loader: 'eslint',
        include: projectRoot,
        exclude: /node_modules/
      }
    ],
 ```
  * les lignes 83 à 85
  ```
  eslint: {
    formatter: require('eslint-friendly-formatter')
  },
  ```

