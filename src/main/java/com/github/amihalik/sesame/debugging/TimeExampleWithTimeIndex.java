package com.github.amihalik.sesame.debugging;

import static java.util.stream.Collectors.toList;

import java.util.Calendar;
import java.util.Date;
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

// Example using Rya's temporal indexing.  Note that the input data looks like this:
//    <s:001> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <t:person> .
//    <s:001> <p:observedAt> "2017-01-01T00:00:00.000-05:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
public class TimeExampleWithTimeIndex {

    public static void main(String[] args) throws Exception {

        Stopwatch watch = Stopwatch.createStarted();

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Creating Connection");

        // The Accumulo In Memory Rya Sail Repo and disable AutoFlushing
        SailRepositoryConnection conn = Utils.getInMemoryAccConn(true);

        ValueFactory vf = new ValueFactoryImpl();

        URI personUri = vf.createURI("t:person");
        URI observedAt = vf.createURI("p:observedAt");

        int numberOfPeople = 1_000;

        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2017, 0, 0);
        long janOne = c.getTimeInMillis();

        List<Statement> people = IntStream.iterate(1, i -> i + 1)
                                    .limit(numberOfPeople)
                                    .mapToObj(i -> vf.createStatement(vf.createURI("s:" + String.format("%03d", i)), RDF.TYPE, personUri))
                                    .collect(toList());

        List<Statement> observedTime = IntStream.iterate(1, i -> i + 1)
                                        .limit(numberOfPeople)
                                        .mapToObj(i -> vf.createStatement(vf.createURI("s:" + String.format("%03d", i)), observedAt, vf.createLiteral(new Date(i * 1000l * 3600l * 24l + janOne))))
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

        String sparql = "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> " 
                      + "SELECT ?s ?t WHERE {  " 
                      + "  ?s <p:observedAt> ?t "
                      + "  FILTER(tempo:after(?t, '2017-02-01T01:01:03-08:00') ) " 
                      + " } ORDER BY ?s";

        System.out.println(conn.prepareTupleQuery(QueryLanguage.SPARQL, sparql));

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Evaluating Query");
        Utils.evaluateAndPrint(sparql, conn);

        sparql = "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> " 
               + "SELECT ?s ?t WHERE {  " 
               + "   ?s <p:observedAt> ?t "
               + "  FILTER(tempo:insideInterval(?t, '[2017-02-01T01:01:03-08:00,2017-05-01T01:01:03-08:00]') ) " 
               + " } ORDER BY ?s";

        System.out.println(conn.prepareTupleQuery(QueryLanguage.SPARQL, sparql));

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Evaluating Query");
        Utils.evaluateAndPrint(sparql, conn);

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Done");
    }
}
