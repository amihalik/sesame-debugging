package com.github.amihalik.sesame.debugging;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.openrdf.model.Statement;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.helpers.QueryResultCollector;
import org.openrdf.query.resultio.text.csv.SPARQLResultsCSVWriter;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;
import org.openrdf.sail.memory.MemoryStore;

public class Utils {

    public static SailRepositoryConnection getInMemoryConn() throws RepositoryException {
        SailRepository s = new SailRepository(new MemoryStore());
        s.initialize();
        SailRepositoryConnection conn = s.getConnection();
        return conn;
    }
    
    public static void flushConnection(SailRepositoryConnection conn) {
        Sail sail = ((SailRepository) conn.getRepository()).getSail();

        if (sail instanceof RdfCloudTripleStore) {
            RdfCloudTripleStore ryaSail = (RdfCloudTripleStore) sail;
            RyaDAO ryaDAO = ryaSail.getRyaDAO();
            if (ryaDAO instanceof AccumuloRyaDAO) {
                AccumuloRyaDAO accRyaDAO = (AccumuloRyaDAO) ryaDAO;
                try {
                    accRyaDAO.flush();
                } catch (RyaDAOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static SailRepositoryConnection getInMemoryAccConn(boolean disableAutoFlushing)
            throws RepositoryException, SailException, AccumuloException, AccumuloSecurityException, RyaDAOException, InferenceEngineException {
        Configuration conf = getConf();
        if (disableAutoFlushing) {
            ((AccumuloRdfConfiguration) conf).setFlush(false);
        }
        conf.setBoolean(ConfigUtils.DISPLAY_QUERY_PLAN, true);

        SailRepository repository = null;
        SailRepositoryConnection conn = null;

        final Sail extSail = RyaSailFactory.getInstance(conf);
        repository = new SailRepository(extSail);
        conn = repository.getConnection();
        return conn;
    }

    public static void evaluateAndPrint(String sparqlQuery, SailRepositoryConnection conn)
            throws TupleQueryResultHandlerException, QueryEvaluationException, MalformedQueryException, RepositoryException {
        conn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery).evaluate(new SPARQLResultsCSVWriter(System.out));

    }

    public static void evaluateAndCount(String sparqlQuery, SailRepositoryConnection conn)
            throws QueryEvaluationException, MalformedQueryException, RepositoryException, QueryResultHandlerException {
        QueryResultCollector c = new QueryResultCollector();
        conn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery).evaluate(c);
        System.out.println("Result Count :: " + c.getBindingSets().size());

    }

    public static void printRepo(SailRepositoryConnection conn) throws RepositoryException {
        RepositoryResult<Statement> rr = conn.getStatements(null, null, null, false);
        while (rr.hasNext()) {
            System.out.println(rr.next());
        }
        System.out.println("Repo size :: " + conn.size());

    }

    private static Configuration getConf() {

        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();

        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
        conf.setBoolean(ConfigUtils.USE_PCJ, false);
        conf.setBoolean(ConfigUtils.USE_FREETEXT, false);
        conf.setBoolean(ConfigUtils.USE_TEMPORAL, true);
        conf.set(PrecomputedJoinIndexerConfig.PCJ_STORAGE_TYPE, PrecomputedJoinStorageType.ACCUMULO.name());
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, "RYA_");
        conf.set(ConfigUtils.CLOUDBASE_USER, "root");
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "");
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, "instance");

        // conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "root");
        // conf.set(ConfigUtils.CLOUDBASE_INSTANCE, "dev");
        // conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, "192.168.33.10");
        
        conf.setInt(ConfigUtils.NUM_PARTITIONS, 3);


        // only geo index statements with geo:asWKT predicates
        conf.set(ConfigUtils.GEO_PREDICATES_LIST, GeoConstants.GEO_AS_WKT.stringValue());
        return conf;
    }
}
