package org.twgogo.jimwayne.logback.ext;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.Map.Entry;

import org.eluder.logback.ext.core.CharacterEncoder;

import com.google.gson.JsonObject;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.spi.ContextAwareBase;

public class GsonEncoder extends ContextAwareBase implements CharacterEncoder<ILoggingEvent> {
  
  private boolean isStarted = false;
  private OutputStream outputStream = null;
  private JsonObject json = null;
  private Charset charset = Charset.forName("UTF-8");
  
  public GsonEncoder () {
    this.json = new JsonObject();
  }
  
  @Override
  public void init (OutputStream os) throws IOException {
    this.outputStream = os;
  }

  @Override
  public void doEncode (ILoggingEvent event) throws IOException {
    // Convert the log event into JSON format.
    this.json.addProperty(
        "logger", 
        event.getLoggerName());
    this.json.addProperty(
        "log_level", 
        event.getLevel().toString());
    this.json.addProperty(
        "thread", 
        event.getThreadName());
    this.json.addProperty(
        "message", 
        event.getFormattedMessage());
    this.json.addProperty(
        "epoch_time", 
        event.getTimeStamp());
    this.json.addProperty(
        "log_level_value", 
        event.getLevel().levelInt);
    
    // Parse the epoch time to ISO-8601 format.
    Instant eventInstant = Instant.ofEpochMilli(event.getTimeStamp());
    ZonedDateTime eventDateTime = 
        ZonedDateTime.ofInstant(
            eventInstant, 
            ZoneId.of(ZoneOffset.UTC.getId()));
    // Add date-time to the log.
    this.json.addProperty("iso_datetime", eventDateTime.toString());
    // Add date to the log.
    this.json.addProperty(
        "iso_date", 
        eventDateTime.toLocalDate().toString());
    
    
    // Parse the MDC contents if there is any.
    Iterator<Entry<String, String>> mdcIter = 
        event.getMDCPropertyMap().entrySet().iterator();
    while (mdcIter.hasNext()) {
      Entry<String, String> mdcEntry = mdcIter.next();
      this.json.addProperty(mdcEntry.getKey(), mdcEntry.getValue());
    }
    
    // Convert the JSON into string, and set the charset.
    byte[] bytes = this.json.toString().getBytes(this.getCharset());
    // Output the JSON string to the OutputStream.
    this.outputStream.write(bytes);
  }

  @Override
  public void close() throws IOException {
    this.outputStream.flush();
  }

  @Override
  public void start() {
    this.isStarted = true;
  }

  @Override
  public void stop() {
    this.isStarted = false;
  }

  @Override
  public boolean isStarted() {
    return this.isStarted;
  }

  @Override
  public void setCharset(Charset charset) {
    if (charset != null) {
      this.charset = charset;
    }
  }

  @Override
  public Charset getCharset() {
    return this.charset;
  }
}
