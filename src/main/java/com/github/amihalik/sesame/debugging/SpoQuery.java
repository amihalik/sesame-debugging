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

public class SpoQuery {
    
    public static void main(String[] args) throws Exception {
        // The Accumulo In Memory Rya Sail Repo behaves as expected.
        SailRepositoryConnection conn = Utils.getInMemoryAccConn(false);

        
        ValueFactory vf = new ValueFactoryImpl();
        
        URI s1 = vf.createURI("u:s1");
        URI o1 = vf.createURI("u:o1");
        URI p = vf.createURI("p:p1");
        
        List<Statement> statements = Arrays.asList( vf.createStatement(s1, p, o1));        

        conn.add(statements);
        
        String sparql = "SELECT * WHERE {  ?s ?p ?o }";
        System.out.println(conn.prepareTupleQuery(QueryLanguage.SPARQL, sparql));
        
        evaluateAndPrint(sparql, conn);
    }

}
