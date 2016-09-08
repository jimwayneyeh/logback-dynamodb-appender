package org.twgogo.jimwayne.logback.ext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
  private static Logger log = LoggerFactory.getLogger(App.class);
  
  public static void main(String[] args) {
    System.out.println("Main: I'm going to write a log.");
    log.info("Hello World!");
  }
}