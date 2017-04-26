package com.github.amihalik.sesame.debugging;

import java.util.concurrent.TimeUnit;

import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.sail.SailRepositoryConnection;

import com.google.common.base.Stopwatch;

public class RangeInvestigation {

    public static void main(String[] args) throws Exception {

        Stopwatch watch = Stopwatch.createStarted();

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Creating Connection");

        // The Accumulo In Memory Rya Sail Repo and disable AutoFlushing
        SailRepositoryConnection conn = Utils.getInMemoryAccConn(false);

        ValueFactory vf = new ValueFactoryImpl();

        conn.add(vf.createStatement(vf.createURI("s:a"), vf.createURI("p:a"), vf.createURI("o:a")));
        conn.add(vf.createStatement(vf.createURI("s:b"), vf.createURI("p:b"), vf.createLiteral("string")));
        conn.add(vf.createStatement(vf.createURI("s:c"), vf.createURI("p:c"), vf.createLiteral("string-lang","english")));
        conn.add(vf.createStatement(vf.createURI("s:d"), vf.createURI("p:d"), vf.createLiteral("string-type",vf.createURI("o:string-type"))));
        
        Utils.printAccumuloTables();
    }
}
