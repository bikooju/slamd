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



import com.slamd.common.Constants;
import com.slamd.job.Job;
import com.slamd.stat.CategoricalTracker;
import com.slamd.stat.StatTracker;
import com.google.gson.Gson;

import com.slamd.parameter.LabelParameter;
import com.slamd.parameter.Parameter;
import com.slamd.parameter.PlaceholderParameter;
import com.slamd.stat.IncrementalTracker;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.LinkedHashMap;
import java.security.MessageDigest;
import java.util.Set;
import java.util.TreeMap;



/**
 * Exports SLAMD job metrics to a PostgreSQL database for Grafana
 * visualization.
 */
public final class MetricsExporter
{
  private static final Map<String, String> TRACKER_PREFIX_TO_OP_TYPE =
      Map.of(
          "adds",     "add",
          "deletes",  "delete",
          "searches", "search",
          "binds",    "bind",
          "modifies", "modify");

  private static final Map<String, String> OP_TYPE_TO_TRACKER_PREFIX =
      Map.of(
          "add",    "Add",
          "delete", "Delete",
          "search", "Search",
          "bind",   "Bind",
          "modify", "Modify");

  private static final Map<String, String> OP_TYPE_TO_PLURAL_PREFIX =
      Map.of(
          "add",    "Adds",
          "delete", "Deletes",
          "search", "Searches",
          "bind",   "Binds",
          "modify", "Modifies");

  private static final Set<String> CONFIG_EXCLUDED_DISPLAY_NAMES =
          Set.of(
                  "Product Name",
                  "Server Addresses and Ports",
                  "Directory Server Address",
                  "Directory Server Port",
                  "Bind DN",
                  "Bind Password",
                  "Search User Bind DN",
                  "Search User Bind Password",
                  "Warm-Up Duration",
                  "Cool-Down Duration");

  private static final String SQL_UPSERT_RUN =
          "INSERT INTO slamd_run " +
                  "(job_id, job_type, job_description, " +
                  " job_class, current_state, " +
                  " start_time, stop_time, " +
                  " duration_seconds, " +
                  " product_name, thread_count, comparison_group) " +
                  "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                  "ON CONFLICT (job_id) DO UPDATE SET " +
                  "  job_type          = EXCLUDED.job_type, " +
                  "  job_description   = EXCLUDED.job_description, " +
                  "  job_class         = EXCLUDED.job_class, " +
                  "  current_state     = EXCLUDED.current_state, " +
                  "  start_time        = EXCLUDED.start_time, " +
                  "  stop_time         = EXCLUDED.stop_time, " +
                  "  duration_seconds  = EXCLUDED.duration_seconds, " +
                  "  product_name      = EXCLUDED.product_name, " +
                  "  thread_count      = EXCLUDED.thread_count, " +
                  "  comparison_group  = EXCLUDED.comparison_group";

  private static final String SQL_UPSERT_OP_SUMMARY =
      "INSERT INTO slamd_op_summary " +
      "(job_id, op_type, total_count, tps, " +
      " avg_latency_ms, std_dev, " +
      " corr_coeff, success_rate) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
      "ON CONFLICT (job_id, op_type) DO UPDATE SET " +
      "  total_count    = EXCLUDED.total_count, " +
      "  tps            = EXCLUDED.tps, " +
      "  avg_latency_ms = EXCLUDED.avg_latency_ms, " +
      "  std_dev        = EXCLUDED.std_dev, " +
      "  corr_coeff     = EXCLUDED.corr_coeff, " +
      "  success_rate   = EXCLUDED.success_rate";

  private static final String SQL_UPSERT_BUCKET =
      "INSERT INTO slamd_op_bucket " +
      "(job_id, op_type, bucket_kind, " +
      " bucket_name, count, percent) " +
      "VALUES (?, ?, ?, ?, ?, ?) " +
      "ON CONFLICT (job_id, op_type, " +
      "  bucket_kind, bucket_name) " +
      "DO UPDATE SET " +
      "  count   = EXCLUDED.count, " +
      "  percent = EXCLUDED.percent";

  private static final String SQL_UPSERT_JOB_PARAMS =
          "INSERT INTO slamd_job_params " +
                  "(job_id, params, config_hash, config_label) " +
                  "VALUES (?, CAST(? AS JSONB), ?, ?) " +
                  "ON CONFLICT (job_id) DO UPDATE SET " +
                  "  params = CAST(EXCLUDED.params AS JSONB), " +
                  "  config_hash = EXCLUDED.config_hash, " +
                  "  config_label = EXCLUDED.config_label";

  private static final String SQL_UPSERT_INTERVAL =
          "INSERT INTO slamd_op_interval " +
                  "(job_id, op_type, collection_interval, " +
                  " counts, ops_per_sec) " +
                  "VALUES (?, ?, ?, ?, ?) " +
                  "ON CONFLICT (job_id, op_type) DO UPDATE SET " +
                  "  collection_interval = EXCLUDED.collection_interval, " +
                  "  counts              = EXCLUDED.counts, " +
                  "  ops_per_sec         = EXCLUDED.ops_per_sec";

  private static final Gson GSON = new Gson();



  /**
   * Prevent instantiation.
   */
  private MetricsExporter()
  {
  }



  /**
   * Exports the given job's metrics to the PostgreSQL database.
   *
   * @param  job  The job whose metrics should be exported.
   */
  public static void export(Job job)
  {
    Properties props = loadConfig();
    if (! "true".equalsIgnoreCase(
        props.getProperty("metrics.db.enabled")))
    {
      return;
    }

    try (Connection conn = getConnection(props))
    {
      conn.setAutoCommit(false);

      upsertRun(conn, job);

      upsertJobParams(conn, job);

      String[] trackerNames = job.getStatTrackerNames();
      List<String> opTypes = detectOpTypes(trackerNames);

      for (String opType : opTypes)
      {
        OpData data = new OpData();

        extractCompleted(job, trackerNames, opType, data);
        extractDuration(job, trackerNames, opType, data);
        extractBuckets(job, trackerNames, opType,
            "Result Codes", "result_code", data);
        extractBuckets(job, trackerNames, opType,
            "Response Time Categories", "response_time",
            data);

        upsertOpSummary(conn, job.getJobID(), opType,
            data.totalCount, data.tps,
            data.avgLatencyMs, data.stdDev,
            data.corrCoeff, data.successRate);

        for (int i = 0; i < data.bucketMeta.size(); i++)
        {
          String[] meta = data.bucketMeta.get(i);
          double[] vals = data.bucketCounts.get(i);
          upsertBucket(conn, job.getJobID(),
              meta[0], meta[1], meta[2],
              (int) vals[0], vals[1]);
        }
      }

      upsertIntervals(conn, job, opTypes);

      conn.commit();

      System.out.println(
          "=== MetricsExporter SUCCESS === " +
          job.getJobID() +
          " (op_types: " + opTypes + ")");
      AdminServlet.slamdServer.logMessage(128,
          "Metrics DB export completed: " +
          job.getJobID() +
          " (op_types: " + opTypes + ")");
    }
    catch (Exception e)
    {
      System.err.println("=== MetricsExporter ERROR ===");
      e.printStackTrace();
      AdminServlet.slamdServer.logMessage(1024,
          "Metrics DB export failed: " + e.getMessage());
    }
  }



  /**
   * Detects which LDAP operation types are present in the stat
   * tracker names.
   *
   * @param  trackerNames  The stat tracker names from the job.
   *
   * @return  A list of detected operation type strings.
   */
  static List<String> detectOpTypes(String[] trackerNames)
  {
    ArrayList<String> opTypes = new ArrayList<>();

    for (String name : trackerNames)
    {
      if (! name.endsWith(" Completed"))
      {
        continue;
      }

      String prefix = name
          .substring(0, name.length() - " Completed".length())
          .trim().toLowerCase();
      String opType = TRACKER_PREFIX_TO_OP_TYPE.get(prefix);

      if (opType != null)
      {
        opTypes.add(opType);
      }
    }

    return opTypes;
  }



  /**
   * Finds a stat tracker name that matches the given operation type
   * and contains the specified keyword.
   *
   * @param  trackerNames  The stat tracker names from the job.
   * @param  opType        The operation type to match.
   * @param  keyword       The keyword to search for.
   *
   * @return  The matching tracker name, or {@code null} if not found.
   */
  static String findTracker(String[] trackerNames,
                            String opType, String keyword)
  {
    String singular = OP_TYPE_TO_TRACKER_PREFIX.get(opType);
    String plural   = OP_TYPE_TO_PLURAL_PREFIX.get(opType);

    for (String name : trackerNames)
    {
      if (name.startsWith(singular + " ") &&
          name.contains(keyword))
      {
        return name;
      }
    }

    for (String name : trackerNames)
    {
      if (name.startsWith(plural + " ") &&
          name.contains(keyword))
      {
        return name;
      }
    }

    for (String name : trackerNames)
    {
      if (name.contains(keyword))
      {
        return name;
      }
    }

    return null;
  }



  /**
   * Extracts the completed-operation count and TPS from the job.
   *
   * @param  job           The job to extract data from.
   * @param  trackerNames  The stat tracker names from the job.
   * @param  opType        The operation type to extract.
   * @param  data          The data object to populate.
   */
  private static void extractCompleted(Job job,
      String[] trackerNames, String opType, OpData data)
  {
    String name = findTracker(trackerNames, opType,
        "Completed");
    if (name == null)
    {
      return;
    }

    StatTracker[] trackers = job.getStatTrackers(name);
    if (trackers.length == 0)
    {
      return;
    }

    StatTracker agg = trackers[0].newInstance();
    agg.aggregate(trackers);

    String[] summary = agg.getSummaryData();
    data.totalCount = parseDouble(summary[0]);
    data.tps        = parseDouble(summary[1]);
  }



  /**
   * Extracts the duration and latency stats from the job.
   *
   * @param  job           The job to extract data from.
   * @param  trackerNames  The stat tracker names from the job.
   * @param  opType        The operation type to extract.
   * @param  data          The data object to populate.
   */
  private static void extractDuration(Job job,
      String[] trackerNames, String opType, OpData data)
  {
    String name = findTracker(trackerNames, opType,
        "Duration");
    if (name == null)
    {
      return;
    }

    StatTracker[] trackers = job.getStatTrackers(name);
    if (trackers.length == 0)
    {
      return;
    }

    StatTracker agg = trackers[0].newInstance();
    agg.aggregate(trackers);

    String[] summary = agg.getSummaryData();
    data.avgLatencyMs = parseDouble(summary[2]);
    data.stdDev       = parseDouble(summary[4]);
    data.corrCoeff    = parseDouble(summary[5]);
  }



  /**
   * Extracts categorical bucket data from the job.
   *
   * @param  job              The job to extract data from.
   * @param  trackerNames     The stat tracker names from the job.
   * @param  opType           The operation type to extract.
   * @param  trackerKeyword   The keyword to find the tracker.
   * @param  bucketKind       The kind of bucket (e.g. result_code).
   * @param  data             The data object to populate.
   */
  private static void extractBuckets(Job job,
      String[] trackerNames, String opType,
      String trackerKeyword, String bucketKind,
      OpData data)
  {
    String name = findTracker(trackerNames, opType,
        trackerKeyword);
    if (name == null)
    {
      return;
    }

    StatTracker[] trackers = job.getStatTrackers(name);
    if (trackers.length == 0)
    {
      return;
    }

    StatTracker agg = trackers[0].newInstance();
    agg.aggregate(trackers);

    if (! (agg instanceof CategoricalTracker))
    {
      return;
    }

    CategoricalTracker catTracker = (CategoricalTracker) agg;
    String[] categories = catTracker.getCategoryNames();
    int[]    counts     = catTracker.getTotalCounts();
    int      total      = catTracker.getTotalCount();

    for (int i = 0; i < categories.length; i++)
    {
      double pct = (total > 0)
          ? ((double) counts[i] / (double) total)
          : 0.0;

      data.bucketMeta.add(
          new String[]{ opType, bucketKind, categories[i] });
      data.bucketCounts.add(
          new double[]{ counts[i], pct });

      if ("result_code".equals(bucketKind) &&
          categories[i].startsWith("0"))
      {
        data.successRate = pct;
      }
    }
  }



  /**
   * Upserts the job run record into the database.
   *
   * @param  conn  The database connection.
   * @param  job   The job to upsert.
   *
   * @throws  Exception  If a database error occurs.
   */
  private static void upsertRun(Connection conn, Job job)
      throws Exception
  {
    try (PreparedStatement ps =
             conn.prepareStatement(SQL_UPSERT_RUN))
    {
      ps.setString(1, job.getJobID());
      ps.setString(2, job.getJobName());
      ps.setString(3,
          Constants.decodeHtmlEntities(job.getJobDescription()));
      ps.setString(4, job.getJobClassName());
      ps.setString(5, job.getJobStateString());

      Date start = job.getActualStartTime();
      ps.setTimestamp(6, start != null
          ? new Timestamp(start.getTime()) : null);

      Date stop = job.getActualStopTime();
      ps.setTimestamp(7, stop != null
          ? new Timestamp(stop.getTime()) : null);

      ps.setInt(8, job.getActualDuration());

      // product_name
      Parameter productNameParam =
              job.getParameterList().getParameter("product_name");
      if (productNameParam != null
              && productNameParam.getValueString() != null &&
      !productNameParam.getValueString().isEmpty())
      {
        ps.setString(9,
            Constants.decodeHtmlEntities(
                productNameParam.getValueString()));
      }
      else
      {
        ps.setNull(9, java.sql.Types.VARCHAR);
      }

      // thread_count = numClients * threadsPerClient
      ps.setInt(10,
              job.getNumberOfClients() * job.getThreadsPerClient());

      // comparison_group (currently not setting - set null)
      ps.setNull(11, java.sql.Types.VARCHAR);

      ps.executeUpdate();
    }
  }



  /**
   * Upserts the per-operation summary into the database.
   *
   * @param  conn          The database connection.
   * @param  jobId         The job ID.
   * @param  opType        The operation type.
   * @param  totalCount    The total operation count.
   * @param  tps           The throughput in operations per second.
   * @param  avgLatencyMs  The average latency in milliseconds.
   * @param  stdDev        The standard deviation.
   * @param  corrCoeff     The correlation coefficient.
   * @param  successRate   The success rate, or {@code null}.
   *
   * @throws  Exception  If a database error occurs.
   */
  private static void upsertOpSummary(Connection conn,
      String jobId, String opType,
      double totalCount, double tps,
      double avgLatencyMs, double stdDev,
      double corrCoeff, Double successRate)
      throws Exception
  {
    try (PreparedStatement ps =
             conn.prepareStatement(SQL_UPSERT_OP_SUMMARY))
    {
      ps.setString(1, jobId);
      ps.setString(2, opType);
      ps.setDouble(3, totalCount);
      ps.setDouble(4, tps);
      ps.setDouble(5, avgLatencyMs);
      ps.setDouble(6, stdDev);
      ps.setDouble(7, corrCoeff);

      if (successRate != null)
      {
        ps.setDouble(8, successRate);
      }
      else
      {
        ps.setNull(8, java.sql.Types.DOUBLE);
      }

      ps.executeUpdate();
    }
  }



  /**
   * Upserts a single bucket row into the database.
   *
   * @param  conn        The database connection.
   * @param  jobId       The job ID.
   * @param  opType      The operation type.
   * @param  bucketKind  The kind of bucket.
   * @param  bucketName  The name of the bucket.
   * @param  count       The count for this bucket.
   * @param  percent     The percentage for this bucket.
   *
   * @throws  Exception  If a database error occurs.
   */
  private static void upsertBucket(Connection conn,
      String jobId, String opType, String bucketKind,
      String bucketName, int count, double percent)
      throws Exception
  {
    try (PreparedStatement ps =
             conn.prepareStatement(SQL_UPSERT_BUCKET))
    {
      ps.setString(1, jobId);
      ps.setString(2, opType);
      ps.setString(3, bucketKind);
      ps.setString(4, bucketName);
      ps.setInt(5, count);
      ps.setDouble(6, percent);
      ps.executeUpdate();
    }
  }

  /**
   * Upserts the interval-level performance data for each op type.
   *
   * @param  conn  The database connection.
   * @param  job   The job to extract interval data from.
   * @param  opTypes  The detected operation types.
   *
   * @throws  Exception  If a database error occurs.
   */
  private static void upsertIntervals(Connection conn, Job job,
                                      List<String> opTypes)
          throws Exception
  {
    String[] trackerNames = job.getStatTrackerNames();

    for (String opType : opTypes)
    {
      String name = findTracker(trackerNames, opType, "Completed");
      if (name == null)
      {
        continue;
      }
      StatTracker[] trackers = job.getStatTrackers(name);
      if (trackers.length == 0)
      {
        continue;
      }

      StatTracker agg = trackers[0].newInstance();
      agg.aggregate(trackers);

      if (!(agg instanceof IncrementalTracker)) {
        continue;
      }

      IncrementalTracker incrementalTracker = (IncrementalTracker) agg;
      int[] counts = incrementalTracker.getIntervalCounts();
      int interval = incrementalTracker.getCollectionInterval();

      if (counts.length == 0) {
        continue;
      }

      // Build PostgreSQL arrays
      Long[] countsArray = new Long[counts.length];
      Double[] opsArray = new Double[counts.length];
      for (int i = 0; i < counts.length; i++)
      {
        countsArray[i] = (long) counts[i];
        opsArray[i] = (double) counts[i] / interval;
      }

      try (PreparedStatement ps =
              conn.prepareStatement(SQL_UPSERT_INTERVAL))
      {
        ps.setString(1, job.getJobID());
        ps.setString(2, opType);
        ps.setInt(3, interval);
        ps.setArray(4, conn.createArrayOf("bigint", countsArray));
        ps.setArray(5, conn.createArrayOf("float8", opsArray));
        ps.executeUpdate();
      }
    }
  }


  /**
   * Upserts the job parameters as JSON into the database.
   *
   * @param  conn  The database connection.
   * @param  job   The job whose parameters to upsert.
   *
   * @throws  Exception  If a database error occurs.
   */
  private static void upsertJobParams(Connection conn,
      Job job)
      throws Exception
  {
    String json = buildParamsJson(job);
    String[] hashAndLabel = buildConfigHashAndLabel(job);

    try(PreparedStatement ps = conn.prepareStatement(SQL_UPSERT_JOB_PARAMS)) {
      ps.setString(1, job.getJobID());
      ps.setString(2, json);
      ps.setString(3, hashAndLabel[0]);
      ps.setString(4, hashAndLabel[1]);
      ps.executeUpdate();
    }
  }

  /**
   * Builds a config hash and label from the job's performance
   * parameters, excluding server-specific fields.  The hash is
   * computed from the full parameter values so that different
   * products tested with the same settings share the same hash.
   * The label truncates values to 50 characters for display in
   * the Grafana dropdown.
   *
   * @param  job  The job whose parameters to hash.
   *
   * @return  A two-element array: [config_hash, config_label].
   */
  private static String[] buildConfigHashAndLabel(Job job) {
    TreeMap<String, String> configParams = new TreeMap<>();

    Parameter[] stubs = job.getParameterStubs().clone().getParameters();
    for (Parameter stub : stubs)
    {
      Parameter p = job.getParameterList().getParameter(stub.getName());
      if (p == null
        || p instanceof PlaceholderParameter
        || p instanceof LabelParameter
        || p.isSensitive())
      {
        continue;
      }

      String displayName = stub.getDisplayName();
      if (CONFIG_EXCLUDED_DISPLAY_NAMES.contains(displayName)) {
        continue;
      }

      String value = p.getValueString();
      configParams.put(displayName, value != null ? value : "");
    }

    StringBuilder hashInput = new StringBuilder();
    StringBuilder labelInput = new StringBuilder();

    for (Map.Entry<String, String> entry : configParams.entrySet())
    {
      if (hashInput.length() > 0)
      {
        hashInput.append(", ");
        labelInput.append(", ");
      }
      hashInput.append(entry.getKey()).append("=").append(entry.getValue());
      String truncated = entry.getValue().length() > 50
              ? entry.getValue().substring(0, 50)
              : entry.getValue();
      labelInput.append(entry.getKey()).append("=").append(truncated);
    }

    String configHash = md5Hex(hashInput.toString());
    String configLabel = labelInput.toString();

    return new String[]{ configHash, configLabel };
  }

  /**
   * Computes the MD5 hex digest of the given string.
   *
   * @param  input  The input string to hash.
   *
   * @return  The 32-character lowercase hex MD5 hash.
   */
  private static String md5Hex(String input)
  {
    try
    {
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] digest = md.digest(input.getBytes("UTF-8"));
      StringBuilder sb = new StringBuilder();
      for (byte b : digest)
      {
        sb.append(String.format("%02x", b & 0xff));
      }
      return sb.toString();
    }
    catch (Exception e)
    {
      throw new RuntimeException("MD5 computation failed", e);
    }
  }



  /**
   * Builds a JSON string of the job's schedule and parameters.
   *
   * @param  job  The job to build JSON for.
   *
   * @return  The JSON string.
   */
  private static String buildParamsJson(Job job) {
    Map<String, Object> root = new LinkedHashMap<>();

    // common sheduling information
    Map<String, Object> schedule = new LinkedHashMap<>();
    schedule.put("Number of Clients", job.getNumberOfClients());
    schedule.put("Threads per Client", job.getThreadsPerClient());
    schedule.put("Thread Startup Delay (ms)", job.getThreadStartupDelay());
    schedule.put("Collection Interval (s)", job.getCollectionInterval());
    schedule.put("Scheduled Duration (s)", job.getDuration());
    schedule.put("Wait for Clients", job.waitForClients());
    schedule.put("Monitor Clients", job.monitorClientsIfAvailable());
    root.put("schedule", schedule);

    Map<String, Object> params = new LinkedHashMap<>();

    Parameter[] stubs = job.getParameterStubs().clone().getParameters();

    for (Parameter stub : stubs) {
      // value exists in parameter
      Parameter p = job.getParameterList().getParameter(stub.getName());

      if (p == null
          || p instanceof PlaceholderParameter
          || p instanceof LabelParameter)
      {
        continue;
      }

      String displayName = stub.getDisplayName();
      String value = p.isSensitive() ? "***" : p.getValueString();

      params.put(displayName, value != null ? value : "");
    }

    root.put("params", params);

    return GSON.toJson(root);
  }



  /**
   * Loads the metrics configuration from the properties file.
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
        AdminServlet.slamdServer.logMessage(1024,
            "Failed to load metrics config: " +
            e.getMessage());
      }
    }

    return props;
  }



  /**
   * Creates a JDBC connection using the given properties.
   *
   * @param  props  The configuration properties.
   *
   * @return  The database connection.
   *
   * @throws  Exception  If a connection error occurs.
   */
  private static Connection getConnection(Properties props)
      throws Exception
  {
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

    String url = "jdbc:postgresql://" + host + ":" + port +
        "/" + dbName;
    Class.forName("org.postgresql.Driver");

    return DriverManager.getConnection(url, user, password);
  }



  /**
   * Parses a numeric string, returning 0.0 if it cannot be parsed.
   *
   * @param  s  The string to parse.
   *
   * @return  The parsed double value, or 0.0.
   */
  private static double parseDouble(String s)
  {
    if (s == null || s.isEmpty())
    {
      return 0.0;
    }

    try
    {
      return Double.parseDouble(s.replace(",", ""));
    }
    catch (NumberFormatException e)
    {
      return 0.0;
    }
  }





  /**
   * Holds intermediate per-operation data during metric extraction.
   */
  private static class OpData
  {
    double totalCount;
    double tps;
    double avgLatencyMs;
    double stdDev;
    double corrCoeff;
    Double successRate;
    final List<String[]> bucketMeta = new ArrayList<>();
    final List<double[]> bucketCounts = new ArrayList<>();
  }
}
