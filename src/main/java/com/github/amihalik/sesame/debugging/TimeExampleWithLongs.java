package com.github.amihalik.sesame.debugging;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.ntriples.NTriplesWriter;

import com.google.common.base.Stopwatch;

//Example Statements
//<s:001> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <t:person> .
//<s:001> <p:observedAt> "1"^^<http://www.w3.org/2001/XMLSchema#long> .
public class TimeExampleWithLongs {

    public static void main(String[] args) throws Exception {
        Stopwatch watch = new Stopwatch();
        watch.start();


        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Creating Connection");

        // The Accumulo In Memory Rya Sail Repo and disable AutoFlushing
        SailRepositoryConnection conn = Utils.getInMemoryAccConn(true);

        ValueFactory vf = new ValueFactoryImpl();

        URI personUri = vf.createURI("t:person");
        URI observedAt = vf.createURI("p:observedAt");

        int numberOfPeople = 1_000;

        List<Statement> people = IntStream.iterate(1, i -> i + 1)
                                    .limit(numberOfPeople)
                                    .mapToObj(i -> vf.createStatement(vf.createURI("s:" + String.format("%03d", i)), RDF.TYPE, personUri))
                                    .collect(toList());

        List<Statement> observedTime = IntStream.iterate(1, i -> i + 1)
                                        .limit(numberOfPeople)
                                        .mapToObj(i -> vf.createStatement(vf.createURI("s:" + String.format("%03d", i)), observedAt, vf.createLiteral((long)i)))
                                        .collect(toList());

        System.out.println(people.size());
        conn.add(people);
        conn.add(observedTime);
        
        System.out.println("Example Statements");

        NTriplesWriter w = new NTriplesWriter(System.out);
        w.startRDF();
        w.handleStatement(people.get(0));
        w.handleStatement(observedTime.get(0));
        w.endRDF();
        

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Done Loading File.  Flushing Accumulo MTBW");

        Utils.flushConnection(conn);

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Preparing Query");

        String sparql = "SELECT ?s ?t WHERE {  " 
                      + "   ?s <p:observedAt> ?t " 
                      + "   FILTER(?t > 10 && ?t < 90) " 
                      + " } ORDER BY ?s";

        System.out.println(conn.prepareTupleQuery(QueryLanguage.SPARQL, sparql));

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Evaluating Query");
        Utils.evaluateAndPrint(sparql, conn);

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Done");
    }
}
