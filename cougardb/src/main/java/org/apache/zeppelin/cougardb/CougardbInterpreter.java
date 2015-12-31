/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.cougardb;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

/**
 * Cougardb interpreter for Zeppelin.
 */
public class CougardbInterpreter extends Interpreter {
  Logger logger = LoggerFactory.getLogger(CougardbInterpreter.class);
  int commandTimeOut = 600000;

  static final String COUGARDBERVER_URL = "cougardb.url";

  static {
    Interpreter.register(
      "sql",
      "cougardb",
      CougardbInterpreter.class.getName(),
      new InterpreterPropertyBuilder()
        .add(COUGARDBERVER_URL, "http://iproxy_dev.idata.oa.com/cql/api", "The API URL for Cougardb")
        .build());
  }

  public CougardbInterpreter(Properties property) {
    super(property);
  }

  Connection jdbcConnection;
  Exception exceptionOnConnect;


  @Override
  public void open() {

  }

  @Override
  public void close() {

  }

  Statement currentStatement;
  private InterpreterResult executeSql(String sql) {
    try {
	  String url = getProperty(COUGARDBERVER_URL);
      OkHttpClient client = new OkHttpClient();

      MediaType mediaType = MediaType.parse("application/json");

      JSONObject obj = new JSONObject();
      obj.put("jsonrpc", "2.0");
      obj.put("id", new Integer(randInt(1, 1000000)));
      obj.put("method", "CougarService.Do");
      obj.put("params", sql);

      StringWriter out = new StringWriter();
      obj.write(out);

      RequestBody body = RequestBody.create(mediaType, out.toString());
      Request request = new Request.Builder()
        .url(url)
        .post(body)
        .addHeader("content-type", "application/json")
        .build();

      Response response = client.newCall(request).execute();

      JSONObject resp_obj = new JSONObject(response.body().string());
      if (!resp_obj.isNull("error")) {
    	JSONObject error = resp_obj.getJSONObject("error");
        return new InterpreterResult(Code.ERROR, error.getString("message"));
      }
      JSONObject result = (JSONObject)resp_obj.get("result");
      JSONArray columns = (JSONArray)result.get("columns");
      JSONArray series = (JSONArray)result.get("series");


      StringBuilder msg = null;
      if (StringUtils.containsIgnoreCase(sql, "EXPLAIN ")) {
        //return the explain as text, make this visual explain later
        msg = new StringBuilder();
      }
      else {
        msg = new StringBuilder("%table ");
      }

        for (int i = 0; i < columns.length() ; i++) {
          if (i == 0) {
            msg.append(columns.getString(i));
          } else {
            msg.append("\t" + columns.getString(i));
          }
        }

        msg.append("\n");


        for (int i = 0; i < series.length() ; i++) {
          JSONArray row = series.getJSONArray(i);

          for (int j = 0; j < row.length() ; j++) {
            if (j == 0) {
              msg.append(row.get(j).toString());
            } else {
              msg.append("\t" + row.get(j).toString());
            }
          }
          msg.append("\n");
        }
      InterpreterResult rett = new InterpreterResult(Code.SUCCESS, msg.toString());
      return rett;
    }
    catch (Exception ex) {
      logger.error("Can not run " + sql, ex);
      return new InterpreterResult(Code.ERROR, ex.getMessage());
    }
  }

  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext contextInterpreter) {
    logger.info("Run SQL command '" + cmd + "'");
    return executeSql(cmd);
  }

  @Override
  public void cancel(InterpreterContext context) {

  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetFIFOScheduler(
        CougardbInterpreter.class.getName() + this.hashCode());
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    return null;
  }

  public static int randInt(int min, int max) {

      Random vrand = new Random();

      // nextInt is normally exclusive of the top value,
      // so add 1 to make it inclusive
      int randomNum = vrand.nextInt((max - min) + 1) + min;

      return randomNum;
  }

}
