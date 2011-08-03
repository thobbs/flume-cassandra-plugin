package org.apache.cassandra.plugins;

import java.io.IOException;
import java.util.Calendar;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.safehaus.uuid.UUID;
import org.safehaus.uuid.UUIDGenerator;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;
import com.cloudera.flume.handlers.syslog.SyslogConsts;

/**
 * Uses Cassandra as a Logsandra compatible sink for syslog messages.
 * 
 * When the sink receives an event, it does the following:
 *
 * 1. Inserts the entry in to the "entries" column family with
 *    a version 1 UUID as the row key.
 * 2. Using the "by_date" column family, indexes the entry by
 *    source, severity, and facility.
 *
 * From there, Logsandra should be able to use the data.  You
 * can find Logsandra at http://github.com/jbohman/logsandra
 */
public class LogsandraSyslogSink extends EventSink.Base {
  
  private static final String KEYSPACE = "logsandra";
  private static final String ENTRIES = "entries";
  private static final String BY_DATE = "by_date";
  
  // Column names
  private static final byte[] IDENT_NAME = "ident".getBytes();
  private static final byte[] SOURCE_NAME = "source".getBytes();
  private static final byte[] DATE_NAME = "date".getBytes();
  private static final byte[] ENTRY_NAME = "entry".getBytes();
    
  private CassandraClient cClient;

  private static final UUIDGenerator uuidGen = UUIDGenerator.getInstance();

  private static final long MILLI_TO_MICRO = 1000; // 1ms = 1000us

  public LogsandraSyslogSink(String[] servers) {
    
    this.cClient = new CassandraClient(KEYSPACE, servers);
  }

  @Override
  public void open() throws IOException {
    this.cClient.open();
  }

  /**
   * Writes the message to Cassandra in a Logsandra compatible way.
 * @throws InterruptedException 
   */
  @Override
  public void append(Event event) throws IOException, InterruptedException {

    long timestamp = System.currentTimeMillis() * MILLI_TO_MICRO;

    UUID uuid = uuidGen.generateTimeBasedUUID();
    
    String date = getDate();
    String host = event.getHost();
    String facility = SyslogConsts.FACILITY[event.get(SyslogConsts.SYSLOG_FACILITY)[0]];
    String severity = SyslogConsts.SEVERITY[event.get(SyslogConsts.SYSLOG_SEVERITY)[0]].toString();
    
    StringBuffer eventInfo = new StringBuffer();
    eventInfo.append(date);
    eventInfo.append(' ');
    eventInfo.append(host);
    eventInfo.append(' ');
    eventInfo.append(facility);
    eventInfo.append(' ');
    eventInfo.append(severity);
    eventInfo.append(' ');
    
    byte[] eventBody = event.getBody();
    byte[] eventInfoBytes = eventInfo.toString().getBytes();
    byte[] finalBytes = new byte[eventInfoBytes.length + eventBody.length];

    for(int i = 0; i < eventInfoBytes.length; i++) {
      finalBytes[i] = eventInfoBytes[i];
    }
    
    for(int i = 0; i < eventBody.length; i++) {
      finalBytes[i + eventInfoBytes.length] = eventBody[i];
    }
    
    Column identColumn = new Column();
    	identColumn.setName(IDENT_NAME);
    	identColumn.setValue("ident".getBytes());
    	identColumn.setTimestamp(timestamp);
    	
    Column sourceColumn = new Column();
			sourceColumn.setName(SOURCE_NAME);
			sourceColumn.setValue(host.getBytes());
			sourceColumn.setTimestamp(timestamp);
			
    Column dateColumn = new Column();
			dateColumn.setName(DATE_NAME);
			dateColumn.setValue(date.getBytes());
			dateColumn.setTimestamp(timestamp);    
    
    Column entryColumn = new Column();
			entryColumn.setName(ENTRY_NAME);
			entryColumn.setValue(finalBytes);
			entryColumn.setTimestamp(timestamp);
    
    Column[] entryColumns = {identColumn, sourceColumn, dateColumn, entryColumn};
    
    Long time = System.currentTimeMillis() * MILLI_TO_MICRO;
    Column timeColumn = new Column();
			timeColumn.setName(toBytes(time));
			timeColumn.setValue(uuid.toString().getBytes());
			timeColumn.setTimestamp(timestamp);
    
    Column[] byDateColumns = {timeColumn};

    // Insert the entry
    this.cClient.insert(uuid.toString().getBytes(), ENTRIES, entryColumns, ConsistencyLevel.QUORUM);
    // Insert the keyword indexes
    this.cClient.insert(
        event.getHost().getBytes(),
        BY_DATE, byDateColumns, ConsistencyLevel.QUORUM);
    this.cClient.insert(
        SyslogConsts.FACILITY[event.get(SyslogConsts.SYSLOG_FACILITY)[0]].getBytes(),
        BY_DATE, byDateColumns, ConsistencyLevel.QUORUM);
    this.cClient.insert(
        SyslogConsts.SEVERITY[event.get(SyslogConsts.SYSLOG_SEVERITY)[0]].toString().getBytes(),
        BY_DATE, byDateColumns, ConsistencyLevel.QUORUM);
    
    super.append(event);
  }

  /** Converts a Long to a big-endian byte[] */
  private byte[] toBytes(Long obj) {
    if (obj == null) {
      return null;
    }
    long l = obj;
    int size = 8;
    byte[] b = new byte[size];
    for (int i = 0; i < size; ++i) {
      b[i] = (byte) (l >> (size - i - 1 << 3));
    }
    return b;
  }

  /**
   * Returns a byte[] representing the current date.
   * This has the format "YYYY-MM-DD HH:MM:SS".
   */
  private String getDate() {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+0"));
    int day = cal.get(Calendar.DAY_OF_MONTH);
    int month = cal.get(Calendar.MONTH);
    int year = cal.get(Calendar.YEAR);
    int hour = cal.get(Calendar.HOUR_OF_DAY);
    int minute = cal.get(Calendar.MINUTE);
    int second = cal.get(Calendar.SECOND);

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
    if(minute < 10)
      buff.append('0');
    buff.append(minute);
    if(second < 10);
      buff.append('0');
    buff.append(second);
    return buff.toString();
  }

  @Override
  public void close() throws IOException {
    this.cClient.close();
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String ... args) {
        if (args.length < 1) {
          throw new IllegalArgumentException(
              "usage: logsandraSyslogSink(\"host:port\"...");
        }
        return new LogsandraSyslogSink(args);
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
    builders.add(new Pair<String, SinkBuilder>("logsandraSyslogSink", builder()));
    return builders;
  }
}
