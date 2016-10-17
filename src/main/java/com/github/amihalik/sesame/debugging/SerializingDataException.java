/*
 * Copyright 2016 Aaron D. Mihalik
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.amihalik.sesame.debugging;

import org.apache.commons.io.IOUtils;
import org.openrdf.rio.ntriples.NTriplesWriter;
import org.openrdf.rio.turtle.TurtleParser;

/**
 * Exception from Email:
 * 
 * <code>
   Caused by: org.openrdf.repository.RepositoryException: org.openrdf.sail.SailException: mvm.rya.api.persist.RyaDAOException: java.io.IOException: mvm.rya.api.resolver.triple.TripleRowResolverException: mvm.rya.api.resolver.RyaTypeResolverException: Exception occurred serializing data[1000000000000]
        at org.openrdf.repository.sail.SailRepositoryConnection.addWithoutCommit(SailRepositoryConnection.java:287)
        at org.openrdf.repository.base.RepositoryConnectionBase.add(RepositoryConnectionBase.java:469)
        at mvm.rya.rdftriplestore.utils.CombineContextsRdfInserter.handleStatement(CombineContextsRdfInserter.java:137)
        at org.openrdf.rio.turtle.TurtleParser.reportStatement(TurtleParser.java:1081)
        at org.openrdf.rio.turtle.TurtleParser.parseObject(TurtleParser.java:482)
        at org.openrdf.rio.turtle.TurtleParser.parseObjectList(TurtleParser.java:405)
        at org.openrdf.rio.turtle.TurtleParser.parsePredicateObjectList(TurtleParser.java:377)
        at org.openrdf.rio.turtle.TurtleParser.parseTriples(TurtleParser.java:362)
        at org.openrdf.rio.turtle.TurtleParser.parseStatement(TurtleParser.java:250)
        at org.openrdf.rio.turtle.TurtleParser.parse(TurtleParser.java:205)
        at org.openrdf.rio.turtle.TurtleParser.parse(TurtleParser.java:148)
        at org.openrdf.repository.util.RDFLoader.loadInputStreamOrReader(RDFLoader.java:325)
        at org.openrdf.repository.util.RDFLoader.load(RDFLoader.java:222)
        at mvm.rya.rdftriplestore.RyaSailRepositoryConnection.add(RyaSailRepositoryConnection.java:61)

 * </code>
 * 
 * link: https://lists.apache.org/thread.html/45e2334582691
 * e9b258b011e8bbd2b9d190e11186d4dcbdd6ae022a5@%3Cdev.rya.apache.org%3E
 *
 */
public class SerializingDataException {

    public static final String TTL = "<foo:foo> <bar:bar> 1000000000000 .";

    public static void main(String[] args) throws Exception {
        TurtleParser p = new TurtleParser();
        p.setRDFHandler(new NTriplesWriter(System.out));
        p.parse(IOUtils.toInputStream(TTL), "");
    }
}
