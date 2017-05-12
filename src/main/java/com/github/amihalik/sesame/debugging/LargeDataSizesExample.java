package com.github.amihalik.sesame.debugging;

import java.util.Base64;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.sail.SailRepositoryConnection;

import com.google.common.base.Stopwatch;

public class LargeDataSizesExample {

    public static void main(String[] args) throws Exception {
        Stopwatch watch = Stopwatch.createStarted();

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Creating Connection");

        // The Accumulo In Memory Rya Sail Repo and disable AutoFlushing
        SailRepositoryConnection conn = Utils.getInMemoryAccConn(true);
        // SailRepositoryConnection conn = Utils.getInMemoryConn();

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Done Creating Connection");

        int statementCount = 1_000;
        int valueSize = 10_000;

        for (int i = 0; i < statementCount; i++) {
            conn.add(getRandomStatement(valueSize));
        }

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Done Loading File.  Flushing Accumulo MTBW");

        Utils.flushConnection(conn);

        // Utils.printRepo(conn);

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Preparing Query");

        String sparql = "SELECT * WHERE {  " + "   ?s <u:predicate> ?o" + " }";

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

    public static Statement getRandomStatement(int urlsize) {
        return VF.createStatement(getRandomURI(urlsize), VF.createURI("u:predicate"), VF.createLiteral(getRandomString(urlsize)));
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
