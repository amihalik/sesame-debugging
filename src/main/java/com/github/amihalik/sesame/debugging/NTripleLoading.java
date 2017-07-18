package com.github.amihalik.sesame.debugging;

import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;

import com.google.common.base.Stopwatch;

public class NTripleLoading {

    public static void main(String[] args) throws Exception {
        Stopwatch watch = new Stopwatch();
        watch.start();

        
        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Creating Connection");

        // The Accumulo In Memory Rya Sail Repo and disable AutoFlushing
        SailRepositoryConnection conn = Utils.getInMemoryAccConn(true);

        final File file = new File("C:/Users/amihalik/Downloads/fb2w.nt.gz");
        
        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Loading File");
        conn.add(new GZIPInputStream(new FileInputStream(file)), file.getName(),RDFFormat.NTRIPLES);
        
        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Done Loading File.  Flushing Accumulo MTBW");

        Utils.flushConnection(conn);
        
        // Utils.printRepo(conn);

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Preparing Query");

        String sparql = "SELECT ?p ?o WHERE {  " 
                      + "   <http://rdf.freebase.com/ns/m.0695j> ?p ?o " 
                      + " }" ;

        System.out.println(conn.prepareTupleQuery(QueryLanguage.SPARQL, sparql));

        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Evaluating Query");
        Utils.evaluateAndPrint(sparql, conn);
        
        System.out.println(watch.elapsed(TimeUnit.SECONDS) + "s Done");
    }
}
