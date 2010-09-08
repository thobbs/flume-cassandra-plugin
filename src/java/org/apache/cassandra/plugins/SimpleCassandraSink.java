package org.apache.cassandra.plugins;

import java.io.IOException;
import java.util.Calendar;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import org.safehaus.uuid.UUID;
import org.safehaus.uuid.UUIDGenerator;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;

/**
 * Allows Cassandra to be used as a sink, primarily for log messages.
 * 
 * When the Cassandra sink receives an event, it does the following:
 *
 * 1. Creates a column where the name is a type 1 UUID (timestamp based) and the
 *    value is the event body.
 * 2. Inserts it into row "YYYYMMDD" (the current date) in the given ColumnFamily.
 *
 * SimpleCassandraSink primarily targets log storage right now.
 */
public class SimpleCassandraSink extends EventSink.Base {

  private ServerSet serverSet;
  private String currentServer;
  
  private String keyspace;
  
  private String dataColumnFamily;
  private ColumnParent dataColumnParent;
  private String indexColumnFamily;
  private ColumnParent indexColumnParent;
  
  private Cassandra.Client client;
  private TTransport transport;

  private static final UUIDGenerator uuidGen = UUIDGenerator.getInstance();

  /** How long we wait before retrying a dead server. */
  private static final long DEFAULT_RETRY_TIME = 500; // 500ms
  private static final long MILLI_TO_MICRO = 1000; // 1ms = 1000us

  public SimpleCassandraSink(String keyspace, String dataColumnFamily,
      String indexColumnFamily, String[] servers) {
    this.keyspace = keyspace;
    this.dataColumnFamily = dataColumnFamily;
    this.dataColumnParent = new ColumnParent(this.dataColumnFamily);
    this.indexColumnFamily = indexColumnFamily;
    this.indexColumnParent = new ColumnParent(this.indexColumnFamily);
    this.serverSet = new ServerSet(servers, DEFAULT_RETRY_TIME);
    this.currentServer = null;
  }

  @Override
  public void open() throws IOException {
    // Initialize the sink
    try {
      this.currentServer = this.serverSet.get();
    } catch (ServerSet.NoServersAvailableException e) {
      throw new IOException("No Cassandra servers available.");
    }

    int splitIndex = this.currentServer.indexOf(':');
    if(splitIndex == -1) {
      throw new IOException("Bad host:port pair: " + this.currentServer);
    }
    String host = this.currentServer.substring(0, splitIndex);
    int port = Integer.parseInt(this.currentServer.substring(splitIndex + 1));

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
   * name is a type 1 UUID, which includes a time stamp
   * component.
   */
  @Override
  public void append(Event event) throws IOException {

    Clock clock = new Clock(System.currentTimeMillis() * MILLI_TO_MICRO);

    // Make the index column
    UUID uuid = uuidGen.generateTimeBasedUUID();
    Column indexColumn = new Column(uuid.toByteArray(), new byte[0], clock);

    // Make the data column
    Column dataColumn = new Column("data".getBytes(), event.getBody(), clock);

    try {
      // Insert the index
      this.client.insert(this.getKey(), this.indexColumnParent, indexColumn, ConsistencyLevel.QUORUM);
      // Insert the data (row key is the uuid and there is only one column)
      this.client.insert(uuid.toString().getBytes(), this.dataColumnParent, dataColumn, ConsistencyLevel.QUORUM);
      super.append(event);
    } catch (Exception exc) {
      this.serverSet.markDead(this.currentServer);
      this.open();
      this.append(event);
    }
  }

  /**
   * Returns a String representing the current date to be used as
   * a key.  This has the format "YYYYMMDDHH".
   */
  private byte[] getKey() {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+0"));
    int day = cal.get(Calendar.DAY_OF_MONTH);
    int month = cal.get(Calendar.MONTH);
    int year = cal.get(Calendar.YEAR);
    int hour = cal.get(Calendar.HOUR_OF_DAY);

    StringBuffer buff = new StringBuffer();
    buff.append(year);
    if(month < 10)
      buff.append('0');
    buff.append(month);
    if(day < 10)
      buff.append('0');
    buff.append(day);
    if(hour < 10)
      buff.append('0');
    buff.append(hour);
    return buff.toString().getBytes();
  }

  @Override
  public void close() throws IOException {
    this.transport.close();
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String ... args) {
        if (args.length < 4) {
          throw new IllegalArgumentException(
              "usage: simpleCassandraSink(\"keyspace\", \"data_column_family\", " +
              "\"index_column_family\", \"host:port\"...");
        }
        String[] servers = Arrays.copyOfRange(args, 3, args.length);
        return new SimpleCassandraSink(args[0], args[1], args[2], servers);
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

  /** Holds a set of servers that may be marked dead and reinstated. */
  private class ServerSet {

    private ArrayList<String> servers;
    private int serverIndex;
    private Queue<Pair> dead = new LinkedList<Pair>();
    private long retryTime;

    ServerSet(String[] servers, long retryTime) {
      this.retryTime = retryTime;

      // Uniformly randomly permute the list()
      Random rand = new Random();
      for(int i=0; i < servers.length; i++) {
        int j = rand.nextInt(servers.length);
        String temp = servers[i];
        servers[i] = servers[j];
        servers[j] = temp;
      }

      this.servers = new ArrayList<String>();
      for(int i=0; i < servers.length; i++) {
        this.servers.add(servers[i]);
      }
      this.serverIndex = 0;
    }

    /** Gets the next available server. */
    String get() throws NoServersAvailableException {
      if(!this.dead.isEmpty()) {
        Pair pair = this.dead.remove();
        if (pair.l > System.currentTimeMillis())
          this.dead.add(pair);
        else
          this.servers.add(pair.str);
      }
      if(this.servers.isEmpty()) {
        throw new NoServersAvailableException();
      }
      return this.servers.get(this.serverIndex++);
    }

    /**
     * Marks the server as dead.  It will be reinstated after retryTime
     * has passed.
     */
    void markDead(String server) {
      this.servers.remove(server);
      this.dead.add(new Pair(server, System.currentTimeMillis() + this.retryTime));
    }

    private class Pair {
      String str;
      Long l;

      Pair(String s, Long l) {
        this.str = s;
        this.l = l;
      }
    }

    public class NoServersAvailableException extends Exception {      
      private static final long serialVersionUID = 1L;
      NoServersAvailableException() {
        super();
      }
    }
  }
}