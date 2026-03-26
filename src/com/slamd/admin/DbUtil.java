/*
 *                             Sun Public License
 *
 * The contents of this file are subject to the Sun Public License Version
 * 1.0 (the "License").  You may not use this file except in compliance with
 * the License.  A copy of the License is available at http://www.sun.com/
 *
 * The Original Code is the SLAMD Distributed Load Generation Engine.
 * The Initial Developer of the Original Code is Neil A. Wilson.
 * Portions created by Neil A. Wilson are Copyright (C) 2004-2019.
 * Some preexisting portions Copyright (C) 2002-2006 Sun Microsystems, Inc.
 * All Rights Reserved.
 *
 * Contributor(s):  Neil A. Wilson
 */
package com.slamd.admin;



import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;



/**
 * Utility class for PostgreSQL database connections.
 * Reads connection info from WEB-INF/slamd_metrics.properties.
 */
public final class DbUtil
{
  /**
   * Private constructor to prevent instantiation.
   */
  private DbUtil()
  {
  }



  /**
   * Creates a JDBC connection to the metrics database.
   *
   * @return  The database connection.
   *
   * @throws  Exception  If a connection error occurs.
   */
  public static Connection getConnection()
      throws Exception
  {
    Properties props = loadConfig();

    String host = props.getProperty(
        "metrics.db.host", "localhost");
    String port = props.getProperty(
        "metrics.db.port", "5432");
    String dbName = props.getProperty(
        "metrics.db.name", "slamd_metrics");
    String user = props.getProperty(
        "metrics.db.user", "slamd");
    String password = props.getProperty(
        "metrics.db.password", "slamd");

    String url = "jdbc:postgresql://" + host + ":"
        + port + "/" + dbName
        + "?charSet=UTF-8";
    Class.forName("org.postgresql.Driver");

    return DriverManager.getConnection(url, user, password);
  }



  /**
   * Loads the metrics configuration from the
   * properties file.
   *
   * @return  The loaded properties.
   */
  private static Properties loadConfig()
  {
    File file = new File(AdminServlet.webInfBasePath,
        "slamd_metrics.properties");
    Properties props = new Properties();

    if (file.exists())
    {
      try (FileInputStream fis = new FileInputStream(file))
      {
        props.load(fis);
      }
      catch (Exception e)
      {
        // Config load failed, use defaults.
      }
    }

    return props;
  }
}
