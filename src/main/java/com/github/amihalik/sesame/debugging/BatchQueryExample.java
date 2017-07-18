package com.github.amihalik.sesame.debugging;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.sail.SailRepositoryConnection;

import com.google.common.base.Stopwatch;


public class BatchQueryExample {

    public static void main(String[] args) throws Exception {
        Stopwatch watch = new Stopwatch();
        watch.start();

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Creating Connection");

        // The Accumulo In Memory Rya Sail Repo and disable AutoFlushing
        SailRepositoryConnection conn = Utils.getInMemoryAccConn(true);
        // SailRepositoryConnection conn = Utils.getInMemoryConn();

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Done Creating Connection");

        int statementCount = 1_000;
        int valueSize = 10;
        
        List<String[]> queryValues = new ArrayList<>();

        for (int i = 0; i < statementCount; i++) {
            Statement statement = getRandomStatement(valueSize);
            conn.add(statement);
            if (i < 10) {
                String s = statement.getSubject().stringValue();
                String p = statement.getPredicate().stringValue();
                String o = statement.getObject().stringValue();
                
                queryValues.add(new String[]{s,p,o});
            }
        }

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Done Loading File.  Flushing Accumulo MTBW");

        Utils.flushConnection(conn);

        // Utils.printRepo(conn);

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Preparing Query");

        String sparql = "SELECT * WHERE {  " + //
                        createValuesSyntax(queryValues) + //
                        "   ?s ?p ?o" + //
                        " }";

        
        System.out.println(conn.prepareTupleQuery(QueryLanguage.SPARQL, sparql));

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Evaluating Query");
        Utils.evaluateAndCount(sparql, conn);
        Utils.evaluateAndPrint(sparql, conn);

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Done");
    }

    private static Random r = new Random();

    public static void setRandomSeed(long seed) {
        r.setSeed(seed);
    }
    
    public static  String createValuesSyntax(List<String[]> values) {
        String format = "VALUES (?s ?p ?o)  { \n%s\n }";
        String valueString = values.stream() //
                .map(a -> String.format("  ( <%s>  <%s>  <%s> ) ", a)) //
                .collect(Collectors.joining("\n"));
        return String.format(format, valueString);

    }

    public static Statement getRandomStatement(int urlsize) {
        return VF.createStatement(getRandomURI(urlsize), getRandomURI(urlsize), getRandomURI(urlsize));
    }

    private static ValueFactory VF = new ValueFactoryImpl();

    public static URI getRandomURI(int length) {
        return VF.createURI("U:" + getRandomString(length));
    }

    public static String getRandomString(int length) {
        int byteArrayLength = length;
        byte[] byteArray = new byte[byteArrayLength];
        r.nextBytes(byteArray);
        String randomString = "r" + Base64.getUrlEncoder().encodeToString(byteArray);
        return randomString.substring(0, length);
    }
}
