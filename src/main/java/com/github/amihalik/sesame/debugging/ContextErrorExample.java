package com.github.amihalik.sesame.debugging;

import static com.github.amihalik.sesame.debugging.Utils.evaluateAndCount;
import static com.github.amihalik.sesame.debugging.Utils.evaluateAndPrint;

import java.util.Arrays;
import java.util.List;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.sail.SailRepositoryConnection;

public class ContextErrorExample {
    
    public static void main(String[] args) throws Exception {
        // The OpenRDF In Memory Sail Repo doesn't seem to behave as expected.
//        SailRepositoryConnection conn = Utils.getInMemoryConn();

        
        // The Accumulo In Memory Rya Sail Repo behaves as expected.
        SailRepositoryConnection conn = Utils.getInMemoryAccConn(false);

        
        ValueFactory vf = new ValueFactoryImpl();
        
        URI s1 = vf.createURI("u:s1");
        URI s2 = vf.createURI("u:s2");
        URI s3 = vf.createURI("u:s3");

        URI o1 = vf.createURI("u:o1");
        URI o2 = vf.createURI("u:o2");
        URI o3 = vf.createURI("u:o3");

        URI c1 = vf.createURI("u:c1");
        URI c2 = vf.createURI("u:c2");
        URI c3 = vf.createURI("u:c3");

        URI p = vf.createURI("p:p1");
        
        List<Statement> statements = Arrays.asList(
                vf.createStatement(s1, p, o1, c1),
                vf.createStatement(s1, p, o2, c2)
                );        

        conn.add(statements);
        
        String sparql = "SELECT ?c ?o WHERE {  " + 
                        " GRAPH ?c {" +
                        "   ?s <p:p1> ?x" + 
                        " }" +
                        " GRAPH ?c {" +
                        "   <u:s1> ?p ?o" + 
                        " }" +
                        "}";
        System.out.println(conn.prepareTupleQuery(QueryLanguage.SPARQL, sparql));
        
        evaluateAndPrint(sparql, conn);
        evaluateAndCount(sparql, conn);
    }

}
