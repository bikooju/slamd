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

import java.io.BufferedReader;
import java.util.Date;

import com.slamd.common.Constants;
import com.slamd.job.Job;
import com.slamd.job.JobClass;
import com.slamd.parameter.InvalidValueException;
import com.slamd.parameter.LabelParameter;
import com.slamd.parameter.Parameter;
import com.slamd.parameter.ParameterList;
import com.slamd.parameter.PlaceholderParameter;
import com.slamd.server.ClientConnection;
import com.slamd.server.ClientListener;
import com.slamd.server.Scheduler;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import com.slamd.server.SLAMDServer;
import com.slamd.db.SLAMDDB;



/**
 * REST API servlet for the SLAMD server.
 * Handles /api/* requests and returns JSON responses.
 */
public class ApiServlet extends HttpServlet {
  /**
   * Serial version UID.
   */
  private static final long serialVersionUID = 1L;


  /**
   * Gson instance for JSON serialization.
   */
  private final Gson gson = new GsonBuilder()
          .setPrettyPrinting().create();


  /**
   * Handles HTTP GET requests.
   *
   * @param request  The HTTP request.
   * @param response The HTTP response.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  protected void doGet(HttpServletRequest request,
                       HttpServletResponse response)
          throws IOException {
    String path = request.getPathInfo();
    if (path == null) {
      path = "/";
    }

    if (path.equals("/server/status")) {
      handleServerStatus(response);
    } else if (path.equals("/runs")) {
      handleRuns(request, response);
    } else if (path.matches("/runs/[^/]+/summary")) {
      String jobId = path.split("/")[2];
      handleRunSummary(jobId, response);
    } else if (path.matches("/runs/[^/]+/intervals")) {
      String jobId = path.split("/")[2];
      handleRunIntervals(jobId, response);
    } else if (path.matches("/runs/[^/]+/buckets")) {
      String jobId = path.split("/")[2];
      handleRunBuckets(jobId, response);
    } else if (path.equals("/compare")) {
      handleCompare(request, response);
    } else if (path.equals("/job-types")) {
      handleJobTypes(response);
    } else if (path.equals("/job-types/params")) {
      String className =
          request.getParameter("className");
      handleJobTypeParams(className, response);
    } else if (path.matches("/jobs/[^/]+/status")) {
      String jobId = path.split("/")[2];
      handleJobStatus(jobId, response);
    } else if (path.equals("/clients")) {
      handleClients(response);
    } else {
      sendError(response, 404, "Not found: " + path);
    }
  }


  /**
   * Handles HTTP POST requests.
   *
   * @param request  The HTTP request.
   * @param response The HTTP response.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  protected void doPost(HttpServletRequest request,
                        HttpServletResponse response)
          throws IOException {
    String path = request.getPathInfo();
    if (path == null) {
      path = "/";
    }

    if (path.equals("/server/init-db")) {
      handleInitDb(response);
    } else if (path.equals("/jobs")) {
      handleCreateJob(request, response);
    } else if (path.matches(
            "/jobs/[^/]+/cancel")) {
      String jobId = path.split("/")[2];
      handleCancelJob(jobId, response);
    } else {
      sendError(response, 404, "Not found: " + path);
    }
  }


  /**
   * Handles HTTP OPTIONS requests for CORS preflight.
   *
   * @param request  The HTTP request.
   * @param response The HTTP response.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  protected void doOptions(HttpServletRequest request,
                           HttpServletResponse response)
          throws IOException {
    setCorsHeaders(response);
    response.setStatus(200);
  }


  /**
   * Handles GET /api/server/status.
   *
   * @param response The HTTP response.
   * @throws IOException If an I/O error occurs.
   */
  private void handleServerStatus(
          HttpServletResponse response)
          throws IOException {
    SLAMDServer server = AdminServlet.slamdServer;
    boolean running = server != null
            && AdminServlet.slamdRunning;

    JsonObject data = new JsonObject();
    data.addProperty("slamdRunning", running);
    data.addProperty("configDBExists", AdminServlet.configDBExists);

    String reason = AdminServlet.unavailableReason;
    if (reason != null && !running) {
      data.addProperty("unavailableReason",
              reason);
    }

    if (running) {
      data.addProperty("pendingJobCount",
              server.getScheduler().getPendingJobCount());
      data.addProperty("runningJobCount",
              server.getScheduler().getRunningJobCount());
    }

    sendSuccess(response, data);
  }


  /**
   * Handles GET /api/runs.
   *
   * @param request  The HTTP request.
   * @param response The HTTP response.
   * @throws IOException If an I/O error occurs.
   */
  private void handleRuns(HttpServletRequest request,
                          HttpServletResponse response)
          throws IOException {
    String product = request.getParameter("product");
    String opType = request.getParameter("op_type");

    StringBuilder sql = new StringBuilder(
            "SELECT r.job_id, r.job_type, "
                    + "r.job_description, "
                    + "r.current_state, r.start_time, "
                    + "r.stop_time, "
                    + "r.duration_seconds, "
                    + "r.product_name, "
                    + "r.num_clients, "
                    + "r.threads_per_client, "
                    + "r.thread_count "
                    + "FROM slamd_run r");

    if (opType != null && !opType.isEmpty()) {
      sql.append(" JOIN slamd_op_summary s"
              + " ON r.job_id = s.job_id");
    }

    List<String> conditions = new ArrayList<>();
    List<String> params = new ArrayList<>();

    if (product != null && !product.isEmpty()) {
      conditions.add("r.product_name = ?");
      params.add(product);
    }
    if (opType != null && !opType.isEmpty()) {
      conditions.add("s.op_type = ?");
      params.add(opType);
    }

    if (!conditions.isEmpty()) {
      sql.append(" WHERE ");
      sql.append(String.join(" AND ", conditions));
    }

    sql.append(" ORDER BY r.start_time DESC");

    try (Connection conn = DbUtil.getConnection();
         PreparedStatement stmt =
                 conn.prepareStatement(sql.toString())) {
      for (int i = 0; i < params.size(); i++) {
        stmt.setString(i + 1, params.get(i));
      }

      ResultSet rs = stmt.executeQuery();
      JsonArray runs = new JsonArray();

      while (rs.next()) {
        JsonObject run = new JsonObject();
        run.addProperty("jobId",
                rs.getString("job_id"));
        run.addProperty("jobType",
                rs.getString("job_type"));
        run.addProperty("jobDescription",
                rs.getString("job_description"));
        run.addProperty("state",
                rs.getString("current_state"));
        run.addProperty("productName",
                rs.getString("product_name"));
        run.addProperty("numClients",
                rs.getInt("num_clients"));
        run.addProperty("threadsPerClient",
                rs.getInt("threads_per_client"));
        run.addProperty("threadCount",
                rs.getInt("thread_count"));
        run.addProperty("durationSeconds",
                rs.getInt("duration_seconds"));

        if (rs.getTimestamp("start_time") != null) {
          run.addProperty("startTime",
                  rs.getTimestamp("start_time")
                          .toString());
        }
        if (rs.getTimestamp("stop_time") != null) {
          run.addProperty("stopTime",
                  rs.getTimestamp("stop_time")
                          .toString());
        }

        runs.add(run);
      }

      JsonObject data = new JsonObject();
      data.add("runs", runs);
      sendSuccess(response, data);
    } catch (Exception e) {
      sendError(response, 500,
              "DB error: " + e.getMessage());
    }
  }


  /**
   * Handles GET /api/runs/{jobId}/summary.
   *
   * @param jobId    The job identifier.
   * @param response The HTTP response.
   * @throws IOException If an I/O error occurs.
   */
  private void handleRunSummary(String jobId,
                                HttpServletResponse response)
          throws IOException {
    String sql =
            "SELECT op_type, total_count, tps, "
                    + "avg_latency_ms, std_dev, "
                    + "corr_coeff, success_rate "
                    + "FROM slamd_op_summary "
                    + "WHERE job_id = ?";

    try (Connection conn = DbUtil.getConnection();
         PreparedStatement stmt =
                 conn.prepareStatement(sql)) {
      stmt.setString(1, jobId);
      ResultSet rs = stmt.executeQuery();
      JsonArray operations = new JsonArray();

      while (rs.next()) {
        JsonObject op = new JsonObject();
        op.addProperty("opType",
                rs.getString("op_type"));
        op.addProperty("totalCount",
                rs.getLong("total_count"));
        op.addProperty("tps",
                rs.getDouble("tps"));
        op.addProperty("avgLatencyMs",
                rs.getDouble("avg_latency_ms"));
        op.addProperty("stdDev",
                rs.getDouble("std_dev"));
        op.addProperty("corrCoeff",
                rs.getDouble("corr_coeff"));
        op.addProperty("successRate",
                rs.getDouble("success_rate"));
        operations.add(op);
      }

      if (operations.size() == 0) {
        sendError(response, 404,
                "No data for job: " + jobId);
        return;
      }

      JsonObject data = new JsonObject();
      data.addProperty("jobId", jobId);
      data.add("operations", operations);
      sendSuccess(response, data);
    } catch (Exception e) {
      sendError(response, 500,
              "DB error: " + e.getMessage());
    }
  }


  /**
   * Handles GET /api/runs/{jobId}/intervals.
   *
   * @param jobId    The job identifier.
   * @param response The HTTP response.
   * @throws IOException If an I/O error occurs.
   */
  private void handleRunIntervals(String jobId,
                                  HttpServletResponse response)
          throws IOException {
    String sql =
            "SELECT op_type, collection_interval, "
                    + "counts, ops_per_sec "
                    + "FROM slamd_op_interval "
                    + "WHERE job_id = ?";

    try (Connection conn = DbUtil.getConnection();
         PreparedStatement stmt =
                 conn.prepareStatement(sql)) {
      stmt.setString(1, jobId);
      ResultSet rs = stmt.executeQuery();
      JsonArray intervals = new JsonArray();

      while (rs.next()) {
        JsonObject interval = new JsonObject();
        interval.addProperty("opType",
                rs.getString("op_type"));
        interval.addProperty("collectionInterval",
                rs.getInt("collection_interval"));

        Array countsArr = rs.getArray("counts");
        Array opsArr = rs.getArray("ops_per_sec");

        if (countsArr != null) {
          Long[] counts =
                  (Long[]) countsArr.getArray();
          JsonArray countsJson = new JsonArray();
          for (Long c : counts) {
            countsJson.add(c);
          }
          interval.add("counts", countsJson);
        }

        if (opsArr != null) {
          Double[] ops =
                  (Double[]) opsArr.getArray();
          JsonArray opsJson = new JsonArray();
          for (Double o : ops) {
            opsJson.add(o);
          }
          interval.add("opsPerSec", opsJson);
        }

        intervals.add(interval);
      }

      if (intervals.size() == 0) {
        sendError(response, 404,
                "No data for job: " + jobId);
        return;
      }

      JsonObject data = new JsonObject();
      data.addProperty("jobId", jobId);
      data.add("intervals", intervals);
      sendSuccess(response, data);
    } catch (Exception e) {
      sendError(response, 500,
              "DB error: " + e.getMessage());
    }
  }


  /**
   * Handles GET /api/runs/{jobId}/buckets.
   *
   * @param jobId    The job identifier.
   * @param response The HTTP response.
   * @throws IOException If an I/O error occurs.
   */
  private void handleRunBuckets(String jobId,
                                HttpServletResponse response)
          throws IOException {
    String sql =
            "SELECT op_type, bucket_kind, "
                    + "bucket_name, count, percent "
                    + "FROM slamd_op_bucket "
                    + "WHERE job_id = ? "
                    + "ORDER BY op_type, bucket_kind, "
                    + "count DESC";

    try (Connection conn = DbUtil.getConnection();
         PreparedStatement stmt =
                 conn.prepareStatement(sql)) {
      stmt.setString(1, jobId);
      ResultSet rs = stmt.executeQuery();

      JsonArray buckets = new JsonArray();
      String currentKey = null;
      JsonObject currentBucket = null;
      JsonArray currentItems = null;

      while (rs.next()) {
        String opType = rs.getString("op_type");
        String bucketKind =
                rs.getString("bucket_kind");
        String key = opType + ":" + bucketKind;

        if (!key.equals(currentKey)) {
          if (currentBucket != null) {
            currentBucket.add("items",
                    currentItems);
            buckets.add(currentBucket);
          }

          currentKey = key;
          currentBucket = new JsonObject();
          currentBucket.addProperty("opType",
                  opType);
          currentBucket.addProperty("bucketKind",
                  bucketKind);
          currentItems = new JsonArray();
        }

        JsonObject item = new JsonObject();
        item.addProperty("name",
                rs.getString("bucket_name"));
        item.addProperty("count",
                rs.getLong("count"));
        item.addProperty("percent",
                rs.getDouble("percent"));
        currentItems.add(item);
      }

      if (currentBucket != null) {
        currentBucket.add("items", currentItems);
        buckets.add(currentBucket);
      }

      if (buckets.size() == 0) {
        sendError(response, 404,
                "No data for job: " + jobId);
        return;
      }

      JsonObject data = new JsonObject();
      data.addProperty("jobId", jobId);
      data.add("buckets", buckets);
      sendSuccess(response, data);
    } catch (Exception e) {
      sendError(response, 500,
              "DB error: " + e.getMessage());
    }
  }


  /**
   * Handles GET /api/compare.
   *
   * @param request  The HTTP request.
   * @param response The HTTP response.
   * @throws IOException If an I/O error occurs.
   */
  private void handleCompare(HttpServletRequest request,
                             HttpServletResponse response)
          throws IOException {
    String configHash =
            request.getParameter("config_hash");
    String opType =
            request.getParameter("op_type");

    if (configHash == null || opType == null) {
      sendError(response, 400,
              "config_hash and op_type are required");
      return;
    }

    String sql =
            "SELECT r.job_id, r.product_name, "
                    + "r.num_clients, "
                    + "r.threads_per_client, "
                    + "r.thread_count, r.start_time, "
                    + "s.tps, s.avg_latency_ms, "
                    + "s.total_count, s.success_rate, "
                    + "p.config_label "
                    + "FROM slamd_run r "
                    + "JOIN slamd_op_summary s "
                    + "ON r.job_id = s.job_id "
                    + "JOIN slamd_job_params p "
                    + "ON r.job_id = p.job_id "
                    + "WHERE p.config_hash = ? "
                    + "AND s.op_type = ? "
                    + "ORDER BY r.product_name, "
                    + "r.start_time";

    try (Connection conn = DbUtil.getConnection();
         PreparedStatement stmt =
                 conn.prepareStatement(sql)) {
      stmt.setString(1, configHash);
      stmt.setString(2, opType);
      ResultSet rs = stmt.executeQuery();

      JsonArray results = new JsonArray();
      String configLabel = null;

      while (rs.next()) {
        if (configLabel == null) {
          configLabel =
                  rs.getString("config_label");
        }

        JsonObject row = new JsonObject();
        row.addProperty("jobId",
                rs.getString("job_id"));
        row.addProperty("productName",
                rs.getString("product_name"));
        row.addProperty("numClients",
                rs.getInt("num_clients"));
        row.addProperty("threadsPerClient",
                rs.getInt("threads_per_client"));
        row.addProperty("threadCount",
                rs.getInt("thread_count"));
        row.addProperty("tps",
                rs.getDouble("tps"));
        row.addProperty("avgLatencyMs",
                rs.getDouble("avg_latency_ms"));
        row.addProperty("totalCount",
                rs.getLong("total_count"));
        row.addProperty("successRate",
                rs.getDouble("success_rate"));

        if (rs.getTimestamp("start_time") != null) {
          row.addProperty("startTime",
                  rs.getTimestamp("start_time")
                          .toString());
        }

        results.add(row);
      }

      if (results.size() == 0) {
        sendError(response, 404,
                "No comparison data found");
        return;
      }

      JsonObject data = new JsonObject();
      data.addProperty("configLabel", configLabel);
      data.addProperty("opType", opType);
      data.add("results", results);
      sendSuccess(response, data);
    } catch (Exception e) {
      sendError(response, 500,
              "DB error: " + e.getMessage());
    }
  }

  /**
   * Handles POST /api/server/init-db.
   * Creates the SLAMD configuration database and
   * starts the server.
   *
   * @param response The HTTP response.
   * @throws IOException If an I/O error occurs.
   */
  private void handleInitDb(
          HttpServletResponse response)
          throws IOException {
    // 1) read-only mode check
    if (AdminServlet.readOnlyMode) {
      sendError(response, 403,
              "Server is in read-only mode."
                      + " Cannot create database");
      return;
    }

    // 2) already exists check
    if (AdminServlet.configDBExists) {
      sendError(response, 409,
              "Configuration database"
                      + " already exists");
      return;
    }

    try {
      // 3) Create the Berkely DB
      //    (same as AdminUi.java line 389)
      SLAMDDB.createDB(
              AdminServlet.configDBDirectory);

      AdminServlet.configDBExists = true;

      // 4) Start the SLAMD server
      //    (same as AdminUi.java line 395-403)
      AdminServlet.slamdServer =
              new SLAMDServer(
                      AdminServlet.adminServlet,
                      AdminServlet.readOnlyMode,
                      AdminServlet.configDBDirectory,
                      AdminServlet.sslKeyStore,
                      AdminServlet.sslKeyStorePassword,
                      AdminServlet.sslTrustStore,
                      AdminServlet.sslTrustStorePassword);

      AdminServlet.scheduler = AdminServlet.slamdServer
              .getScheduler();
      AdminServlet.configDB = AdminServlet.slamdServer
              .getConfigDB();

      AdminServlet.configDB.registerAsSubscriber(
              AdminConfig.ADMIN_CONFIG);

      AdminConfig.ADMIN_CONFIG
              .refreshSubscriberConfiguration();

      AdminServlet.slamdRunning = true;

      // 5) Success Response
      JsonObject data = new JsonObject();
      data.addProperty("message",
              "Configuration database created"
                      + " and server started"
                      + " successfully.");
      sendSuccess(response, data);
    } catch (Exception e) {
      sendError(response, 500,
              "Failed to create database: "
                      + e.getMessage());
    }
  }

  /**
   * Handles GET /api/job-types.
   *
   * @param  response   The HTTP response.
   *
   * @throws  IOException  If an I/O error occurs.
   */
  private void handleJobTypes(
      HttpServletResponse response)
      throws IOException
  {
    SLAMDServer server =
        AdminServlet.slamdServer;

    if (server == null)
    {
      sendError(response, 503,
          "SLAMD server is not running.");
      return;
    }

    JsonArray jobClassesJson = new JsonArray();
    JobClass[] jobClasses =
        server.getJobClasses("LDAP");

    for (JobClass jobClass : jobClasses)
    {
      JsonObject obj = new JsonObject();
      obj.addProperty("className",
          jobClass.getClass().getName());
      obj.addProperty("name",
          jobClass.getJobName());
      obj.addProperty("description",
          jobClass.getShortDescription());
      jobClassesJson.add(obj);
    }

    JsonObject categoryObj = new JsonObject();
    categoryObj.addProperty("name", "LDAP");
    categoryObj.add("jobClasses",
        jobClassesJson);

    JsonArray categoriesJson = new JsonArray();
    categoriesJson.add(categoryObj);

    JsonObject data = new JsonObject();
    data.add("categories", categoriesJson);
    sendSuccess(response, data);
  }

  /**
   * Handles GET /api/job-types/{className}/params.
   *
   * @param  className  The job class name.
   * @param  response   The HTTP response.
   *
   * @throws  IOException  If an I/O error occurs.
   */
  private void handleJobTypeParams(
          String className,
          HttpServletResponse response)
          throws IOException {
    SLAMDServer server =
        AdminServlet.slamdServer;
    if (server == null)
    {
      sendError(response, 503,
          "SLAMD server is not running.");
      return;
    }

    if (className == null
        || className.isEmpty())
    {
      sendError(response, 400,
          "Missing required parameter:"
              + " className");
      return;
    }

    JobClass jobClass =
        server.getJobClass(className);
    if (jobClass == null)
    {
      sendError(response, 404,
              "Job class not found: "
                      + className);
      return;
    }

    ParameterList stubs =
            jobClass.getParameterStubs();
    Parameter[] params = stubs.getParameters();
    JsonArray paramsJson = new JsonArray();

    for (Parameter param : params) {
      if (param instanceof LabelParameter
              || param instanceof PlaceholderParameter) {
        continue;
      }

      JsonObject paramObj = new JsonObject();
      paramObj.addProperty("name",
              param.getName());
      paramObj.addProperty("displayName",
              param.getDisplayName());
      paramObj.addProperty("type",
              getParameterType(param));
      paramObj.addProperty("required",
              param.isRequired());
      paramObj.addProperty("sensitive",
              param.isSensitive());

      String value = param.getValueString();
      if (value != null) {
        paramObj.addProperty(
                "defaultValue", value);
      }

      paramsJson.add(paramObj);
    }

    JsonObject data = new JsonObject();
    data.addProperty("className",
            className);
    data.addProperty("jobName",
            jobClass.getJobName());
    data.add("parameters", paramsJson);
    sendSuccess(response, data);
  }

  /**
   * Returns the simplified type name for a
   * parameter.
   *
   * @param  param  The parameter.
   *
   * @return  The type name string.
   */
  private String getParameterType(Parameter param) {
    String name =
            param.getClass().getSimpleName();

    if ("IntegerParameter".equals(name)) {
      return "integer";
    } else if ("PasswordParameter".equals(name)) {
      return "password";
    } else if ("BooleanParameter".equals(name)) {
      return "boolean";
    } else if ("MultiChoiceParameter".equals(name)) {
      return "choice";
    } else if ("FileURLParameter".equals(name)) {
      return "url";
    } else if ("MultiLineParameter".equals(name)) {
      return "text";
    }

    return "string";
  }

  /**
   * Handles POST /api/jobs.
   *
   * @param request  The HTTP request.
   * @param response The HTTP response.
   * @throws IOException If an I/O error occurs.
   */
  private void handleCreateJob(
          HttpServletRequest request,
          HttpServletResponse response)
          throws IOException {
    SLAMDServer server =
            AdminServlet.slamdServer;
    if (server == null) {
      sendError(response, 503,
              "SLAMD server is not running");
      return;
    }

    request.setCharacterEncoding("UTF-8");

    StringBuilder body = new StringBuilder();
    try (BufferedReader reader =
                 request.getReader()) {
      String line = reader.readLine();
      while (line != null) {
        body.append(line);
        line = reader.readLine();
      }
    }

    JsonObject reqJson = gson.fromJson(
            body.toString(), JsonObject.class);

    String className = reqJson.get(
            "className").getAsString();
    int numClients = reqJson.get(
            "numClients").getAsInt();
    int threadsPerClient = reqJson.get(
            "threadsPerClient").getAsInt();
    int duration = reqJson.get(
            "duration").getAsInt();

    // default value
    int collectionInterval = 5;
    if (reqJson.has("collectionInterval")) {
      collectionInterval = reqJson.get(
              "collectionInterval").getAsInt();
    }

    String description = "";
    if (reqJson.has("description")) {
      description = reqJson.get(
              "description").getAsString();
    }

    String folderName =
        Constants.FOLDER_NAME_UNCLASSIFIED;
    if (reqJson.has("folderName"))
    {
      folderName = reqJson.get(
          "folderName").getAsString();
    }

    JobClass jobClass =
            server.getJobClass(className);
    if (jobClass == null) {
      sendError(response, 400,
              "Job class not found: "
                      + className);
      return;
    }

    ParameterList stubs =
            jobClass.getParameterStubs();
    Parameter[] params =
            stubs.clone().getParameters();

    if (reqJson.has("parameters")) {
      JsonObject paramValues =
              reqJson.getAsJsonObject(
                      "parameters");
      for (Parameter param : params) {
        String paramName = param.getName();
        if (paramValues.has(paramName)) {
          try {
            param.setValueFromString(
                    paramValues.get(paramName)
                            .getAsString());
          } catch (InvalidValueException e) {
            sendError(response, 400,
                    "Invalid value for "
                    + paramName + ": "
                    + e.getMessage());
            return;
          }
        }
      }
    }

    ParameterList parameterList =
            new ParameterList(params);

    try {
      Job job = new Job(
              server, className,
              numClients, threadsPerClient,
              0, new Date(), null,
              duration, collectionInterval,
              parameterList, false);

      job.setJobDescription(description);

      Scheduler scheduler =
              server.getScheduler();
      String jobId = scheduler.scheduleJob(
              job, folderName);

      JsonObject data = new JsonObject();
      data.addProperty("jobId", jobId);
      data.addProperty("state",
              job.getJobStateString());
      sendSuccess(response, data);
    } catch (Exception e) {
      sendError(response, 500,
              "Failed to schedule job: "
                      + e.getMessage());
    }
  }

    /**
     * Handles GET /api/jobs/{jobId}/status.
     *
     * @param jobId The job identifier.
     * @param response The HTTP response.
     *
     * @throws IOException If an I/O error occurs.
     */
    private void handleJobStatus (String jobId,
            HttpServletResponse response)
    throws IOException
    {
      SLAMDServer server =
              AdminServlet.slamdServer;
      if (server == null) {
        sendError(response, 503,
                "SLAMD server is not running");
        return;
      }

      try {
        Job job =
                server.getScheduler().getJob(jobId);
        if (job == null) {
          sendError(response, 404,
                  "Job not found: " + jobId);
          return;
        }

        JsonObject data = new JsonObject();
        data.addProperty("jobId", jobId);
        data.addProperty("jobName",
                job.getJobName());
        data.addProperty("state",
                job.getJobStateString());
        data.addProperty("stateCode",
                job.getJobState());
        data.addProperty("numClients",
                job.getNumberOfClients());
        data.addProperty(
                "threadsPerClient",
                job.getThreadsPerClient());
        data.addProperty("duration",
                job.getDuration());

        if (job.getActualStartTime() != null) {
          data.addProperty("startTime",
                  job.getActualStartTime()
                          .toString());
        }
        if (job.getActualStopTime() != null) {
          data.addProperty("stopTime",
                  job.getActualStopTime()
                          .toString());
        }
        if (job.getActualDuration() > 0) {
          data.addProperty(
                  "actualDuration",
                  job.getActualDuration());
        }

        sendSuccess(response, data);
      } catch (Exception e) {
        sendError(response, 500,
                "Error: " + e.getMessage());
      }
    }

  /**
   * Handles POST /api/jobs/{jobId}/cancel.
   *
   * @param jobId The job identifier.
   * @param response The HTTP response.
   *
   * @throws IOException If an I/O error occurs.
   */
  private void handleCancelJob(
          String jobId,
          HttpServletResponse response)
    throws IOException
  {
    SLAMDServer server =
            AdminServlet.slamdServer;
    if (server == null) {
      sendError(response, 503,
              "SLAMD server is not running");
      return;
    }

    // Cancelled job
    Job job = server.getScheduler()
            .cancelJob(jobId, true);
    if (job == null) {
      sendError(response, 404,
              "Job not found: " + jobId);
      return;
    }

    JsonObject data = new JsonObject();
    data.addProperty("jobId", jobId);
    data.addProperty("state",
            job.getJobStateString());
    data.addProperty("message",
            "Cancel request submitted");
    sendSuccess(response, data);
  }

  /**
   * Handles GET /api/clients.
   *
   * @param response The HTTP response.
   *
   * @throws IOException If an I/O error occurs.
   */
  private void handleClients(
          HttpServletResponse response)
    throws IOException
  {
    SLAMDServer server =
            AdminServlet.slamdServer;
    if (server == null) {
      sendError(response, 503,
              "SLAMD server is not running");
      return;
    }

    ClientListener listener =
            server.getClientListener();
    ClientConnection[] connections =
            listener.getConnectionList();
    JsonArray clientsJson = new JsonArray();

    for (ClientConnection conn : connections)
    {
      JsonObject client = new JsonObject();
      client.addProperty("clientId",
              conn.getClientID());
      client.addProperty("address",
              conn.getClientIPAddress());
      client.addProperty("connectionId",
              conn.getConnectionID());

      String status =
              (conn.getJob() != null)
                ? "running" : "idle";
      client.addProperty("status", status);

      if (conn.getEstablishedTime() != null)
      {
        client.addProperty("connectedAt",
                conn.getEstablishedTime()
                .toString());
      }

      clientsJson.add(client);
    }

    JsonObject data = new JsonObject();
    data.addProperty("totalClients",
            connections.length);
    data.add("clients", clientsJson);
    sendSuccess(response, data);
  }









    /**
     * Sends a success JSON response.
     *
     * @param  response  The HTTP response.
     * @param  data      The response data.
     *
     * @throws IOException  If an I/O error occurs.
     */
    private void sendSuccess (
            HttpServletResponse response, JsonObject data)
      throws IOException
    {
      JsonObject result = new JsonObject();
      result.addProperty("status", true);
      result.add("data", data);
      writeJson(response, 200, result);
    }


    /**
     * Sends an error JSON response.
     *
     * @param  response    The HTTP response.
     * @param  httpStatus  The HTTP status code.
     * @param  message     The error message.
     *
     * @throws IOException  If an I/O error occurs.
     */
    private void sendError (
            HttpServletResponse response,
    int httpStatus, String message)
      throws IOException
    {
      JsonObject result = new JsonObject();
      result.addProperty("status", false);
      result.addProperty("message", message);
      writeJson(response, httpStatus, result);
    }


    /**
     * Writes a JSON object to the HTTP response.
     *
     * @param  response    The HTTP response.
     * @param  httpStatus  The HTTP status code.
     * @param  json        The JSON object to write.
     *
     * @throws IOException  If an I/O error occurs.
     */
    private void writeJson (
            HttpServletResponse response,
    int httpStatus, JsonObject json)
      throws IOException
    {
      setCorsHeaders(response);
      response.setStatus(httpStatus);
      response.setContentType(
              "application/json; charset=UTF-8");

      PrintWriter writer = response.getWriter();
      writer.print(gson.toJson(json));
      writer.flush();
    }


    /**
     * Sets CORS headers on the response.
     *
     * @param  response  The HTTP response.
     */
    private void setCorsHeaders (
            HttpServletResponse response)
    {
      response.setHeader(
              "Access-Control-Allow-Origin", "*");
      response.setHeader(
              "Access-Control-Allow-Methods",
              "GET, POST, OPTIONS");
      response.setHeader(
              "Access-Control-Allow-Headers",
              "Content-Type");
    }
}
