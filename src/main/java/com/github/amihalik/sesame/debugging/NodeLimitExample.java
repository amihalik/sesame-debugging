package com.github.amihalik.sesame.debugging;

import static com.github.amihalik.sesame.debugging.Utils.evaluateAndCount;
import static com.github.amihalik.sesame.debugging.Utils.evaluateAndPrint;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.sail.SailRepositoryConnection;

public class NodeLimitExample {
    
    public static void main(String[] args) throws Exception {
        // The OpenRDF In Memory Sail Repo doesn't seem to behave as expected.
        //SailRepositoryConnection conn = Utils.getInMemoryConn();

        
        // The Accumulo In Memory Rya Sail Repo behaves as expected.
        SailRepositoryConnection conn = Utils.getInMemoryAccConn(false);

        
        ValueFactory vf = new ValueFactoryImpl();
        
        URI personUri = vf.createURI("t:person");
        URI talksTo = vf.createURI("p:talksTo");
        
        List<Statement> people = IntStream.iterate(1, i -> i + 1).limit(100)
                                                   .mapToObj(i -> vf.createStatement(vf.createURI("s:"+String.format("%03d",i)), RDF.TYPE, personUri))
                                                   .collect(toList());
        
        Stream<Statement> talkStream = people.stream().map(Statement::getSubject)
                                                   .flatMap(subj -> people.stream().map(statement -> vf.createStatement(subj, talksTo, statement.getSubject() )));
        System.out.println(people.size());
        conn.add(people);
        conn.add(talkStream::iterator);

        String sparql = "SELECT * WHERE {  " + 
                        "   { SELECT * WHERE { ?s a <t:person> } LIMIT 10 } " + 
                        "   { SELECT * WHERE { ?s <p:talksTo> ?o . ?o a <t:person> } LIMIT 12 } " + 
                        "}";
        System.out.println(conn.prepareTupleQuery(QueryLanguage.SPARQL, sparql));
        
        evaluateAndPrint(sparql, conn);
        evaluateAndCount(sparql, conn);
    }

}
