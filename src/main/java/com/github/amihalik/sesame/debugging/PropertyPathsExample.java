package com.github.amihalik.sesame.debugging;

import java.util.Random;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.sail.SailRepositoryConnection;

/*
 * There are many more property path examples at: http://stackoverflow.com/a/26707541
 */

public class PropertyPathsExample {

    public static void main(String[] args) throws Exception {
        // The OpenRDF In Memory Sail Repo
        // SailRepositoryConnection conn = Utils.getInMemoryConn();

        // The Accumulo In Memory Rya Sail Repo
        SailRepositoryConnection conn = Utils.getInMemoryAccConn();

        int depth = 10;

        ValueFactory vf = new ValueFactoryImpl();

        URI causes = vf.createURI("p:causes");

        final Random r = new Random(1);

        IntFunction<URI> createUri = i -> vf.createURI("s:" + String.format("%03d", i));

        // for every positive int k that is less than i, randomly create a "caused by" relation between k and i
        IntFunction<Stream<Statement>> createPrecedingEventLink = i -> {
            return IntStream.iterate(1, k -> k + 1)
                            .limit(i - 1)
                            .filter(k -> r.nextFloat() > .5)
                            .mapToObj(k -> vf.createStatement(createUri.apply(k), causes, createUri.apply(i)));
        };

        Stream<Statement> events = IntStream.iterate(1, i -> i + 1)
                                            .limit(depth)
                                            .mapToObj(createPrecedingEventLink)
                                            .flatMap(i -> i);

        conn.add(events::iterator);

        // Utils.printRepo(conn);

        String sparql = "SELECT DISTINCT ?s ?o WHERE {  " 
                      + "    VALUES ?root { <s:001> }"
                      + "    { ?root (<p:causes>*) ?s  . ?s <p:causes> ?o }" 
                      + "      UNION  "
                      + "    { ?root (^<p:causes>*)  ?o . ?s <p:causes> ?o }" 
                      + " }" 
                      + " ORDERBY ?s";

        System.out.println(conn.prepareTupleQuery(QueryLanguage.SPARQL, sparql));

        Utils.evaluateAndPrint(sparql, conn);
        Utils.evaluateAndCount(sparql, conn);
    }
}
