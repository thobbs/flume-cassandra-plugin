package org.apache.cassandra.plugins;
import java.io.IOException;
import java.util.Calendar;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import org.safehaus.uuid.UUIDGenerator;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;

public class SimpleCassandraSink extends EventSink.Base {

    public Cassandra.Client client;
    private String host;
    private int port;
    private String keyspace;
    private String columnFamily;
    private ColumnParent columnParent;
    private TTransport transport;

    private static final UUIDGenerator uuidGen = UUIDGenerator.getInstance();

    public SimpleCassandraSink(String host, int port, String keyspace, String columnFamily) {
        this.host = host;
        this.port = port;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.columnParent = new ColumnParent(this.columnFamily);
    }

    @Override
    public void open() throws IOException {
      // Initialize the sink
        TSocket sock = new TSocket(host, port);
        this.transport = new TFramedTransport(sock);
        TProtocol protocol = new TBinaryProtocol(transport);
        this.client = new Cassandra.Client(protocol);
        try {
            this.transport.open();
            this.client.set_keyspace(this.keyspace);
        } catch(TException texc) {
            throw new IOException(texc.getMessage());
        } catch(InvalidRequestException exc) {
            throw new IOException(exc.getMessage());
        }
    }

    /**
     * Writes the message to Cassandra.
     * The key is the current date (YYYYMMDD) and the column
     * name is a type 1 UUID, which includes a timestamp
     * component.
     */
    @Override
    public void append(Event event) throws IOException {
      byte[] column_name = uuidGen.generateTimeBasedUUID().asByteArray();
      Column column = new Column(column_name, event.getBody(), new Clock(0));
      try {
        this.client.insert(this.getKey(), this.columnParent, column, ConsistencyLevel.QUORUM);
        super.append(event);
      } catch (Exception exc) {
          throw new IOException(exc.getMessage());
      }
    }

    /**
     * Returns a String representing the current date to be used as
     * a key.  This has the format "YYYYMMDD".
     */
    private byte[] getKey() {
      Calendar cal = Calendar.getInstance();
      int day = cal.get(Calendar.DAY_OF_MONTH);
      int month = cal.get(Calendar.MONTH);
      int year = cal.get(Calendar.YEAR);

      StringBuffer buff = new StringBuffer();
      buff.append(year);
      if(month < 10)
        buff.append('0');
      buff.append(month);
      if(day < 10)
        buff.append('0');
      buff.append(day);
      return buff.toString().getBytes();
    }

    @Override
    public void close() throws IOException {
      this.transport.close();
    }


  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... args) {
        if (args.length < 3) {
          throw new IllegalArgumentException(
              "usage: cassandra(\"host\", port, \"keyspace\", \"column_family\"");
        }
        return new SimpleCassandraSink(args[0], Integer.parseInt(args[1]), args[2], args[3]);
      }
    };
  }

    /**
     * This is a special function used by the SourceFactory to pull in this class
     * as a plugin sink.
     */
    public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
      List<Pair<String, SinkBuilder>> builders =
        new ArrayList<Pair<String, SinkBuilder>>();
      builders.add(new Pair<String, SinkBuilder>("simpleCassandraSink", builder()));
      return builders;
    }
}