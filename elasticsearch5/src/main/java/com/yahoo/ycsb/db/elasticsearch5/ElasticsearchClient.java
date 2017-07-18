/**
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db.elasticsearch5;

import com.yahoo.ycsb.*;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import static org.elasticsearch.common.settings.Settings.Builder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Elasticsearch client for YCSB framework.
 */
public class ElasticsearchClient extends DB {

  private static final String DEFAULT_CLUSTER_NAME = "es.ycsb.cluster";
  private static final String DEFAULT_INDEX_KEY = "es.ycsb";
  private static final String DEFAULT_REMOTE_HOST = "localhost:9300";
  private static final int NUMBER_OF_SHARDS = 1;
  private static final int NUMBER_OF_REPLICAS = 0;
  private Node node;
  private Client client;
  private String indexKey;
  private Boolean remoteMode;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    final Properties props = getProperties();

    // Check if transport client needs to be used (To connect to multiple elasticsearch nodes)
    remoteMode = Boolean.parseBoolean(props.getProperty("es.remote", "false"));

    final String pathHome = props.getProperty("path.home");

    // when running in embedded mode, require path.home
    if (!remoteMode && (pathHome == null || pathHome.isEmpty())) {
      throw new IllegalArgumentException("path.home must be specified when running in embedded mode");
    }

    this.indexKey = props.getProperty("es.index.key", DEFAULT_INDEX_KEY);

    int numberOfShards = parseIntegerProperty(props, "es.number_of_shards", NUMBER_OF_SHARDS);
    int numberOfReplicas = parseIntegerProperty(props, "es.number_of_replicas", NUMBER_OF_REPLICAS);

    Boolean newdb = Boolean.parseBoolean(props.getProperty("es.newdb", "false"));
    Builder settings = Settings.builder();

    // if properties file contains elasticsearch user defined properties
    // add it to the settings file (will overwrite the defaults).
    
    final String clusterName = props.getProperty("cluster.name", DEFAULT_CLUSTER_NAME);
    System.err.println("Elasticsearch starting node = " + clusterName);
    System.err.println("Elasticsearch node path.home = " + pathHome);
    System.err.println("Elasticsearch Remote Mode = " + remoteMode);
    // Remote mode support for connecting to remote elasticsearch cluster
    if(remoteMode) {
      RestClient restClient = RestClient.builder(
          new HttpHost("localhost", 9200, "http")).build();
    }
    if (remoteMode) {
      settings.put("client.transport.sniff", true)
          .put("client.transport.ignore_cluster_name", false)
          .put("client.transport.ping_timeout", "30s")
          .put("client.transport.nodes_sampler_interval", "30s")
          .put("cluster.name", clusterName);
      // Default it to localhost:9300
      String[] nodeList = props.getProperty("es.hosts.list", DEFAULT_REMOTE_HOST).split(",");
      System.out.println("Elasticsearch Remote Hosts = " + props.getProperty("es.hosts.list", DEFAULT_REMOTE_HOST));
      TransportClient tClient = new PreBuiltTransportClient(settings.build());
      for (String h : nodeList) {
        String[] nodes = h.split(":");
        try {
          tClient.addTransportAddress(new InetSocketTransportAddress(
              InetAddress.getByName(nodes[0]),
              Integer.parseInt(nodes[1])
          ));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Unable to parse port number.", e);
        } catch (UnknownHostException e) {
          throw new IllegalArgumentException("Unable to Identify host.", e);
        }
      }
      client = tClient;
    } else { // Start node only if transport client mode is disabled
      settings.put(props);
      settings.put("transport.type", "local");
      settings.put("http.enabled", "false");
      node = new Node(settings.build());
      try {
        node.start();
      } catch (NodeValidationException e) {
        throw new DBException(e);
      }
      client = node.client();
    }

    final boolean exists =
        client.admin().indices()
            .exists(Requests.indicesExistsRequest(indexKey)).actionGet()
            .isExists();
    if (exists && newdb) {
      client.admin().indices().prepareDelete(indexKey).execute().actionGet();
    }
    if (!exists || newdb) {
      client.admin().indices().create(
          new CreateIndexRequest(indexKey)
              .settings(
                  Settings.builder()
                      .put("index.number_of_shards", numberOfShards)
                      .put("index.number_of_replicas", numberOfReplicas)
              )).actionGet();
    }
    client.admin().cluster().health(new ClusterHealthRequest().waitForGreenStatus()).actionGet();
  }

  private int parseIntegerProperty(Properties properties, String key, int defaultValue) {
    String value = properties.getProperty(key);
    return value == null ? defaultValue : Integer.parseInt(value);
  }

  @Override
  public void cleanup() throws DBException {
    if (client != null) {
      client.close();
      client = null;
    }

    if (!remoteMode && node != null && !node.isClosed()) {
      try {
        node.close();
        node = null;
      } catch (IOException e) {
        throw new DBException(e);
      }
    }
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    try {
      final XContentBuilder doc = jsonBuilder().startObject();

      for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        doc.field(entry.getKey(), entry.getValue());
      }

      doc.endObject();

      client.prepareIndex(indexKey, table, key).setSource(doc).execute().actionGet();

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      DeleteResponse response = client.prepareDelete(indexKey, table, key).execute().actionGet();
      if (response.status().equals(RestStatus.NOT_FOUND)) {
        return Status.NOT_FOUND;
      } else {
        return Status.OK;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    try {
      final GetResponse response = client.prepareGet(indexKey, table, key).execute().actionGet();

      if (response.isExists()) {
        if (fields != null) {
          for (String field : fields) {
            result.put(field, new StringByteIterator(
                (String) response.getSource().get(field)));
          }
        } else {
          for (String field : response.getSource().keySet()) {
            result.put(field, new StringByteIterator(
                (String) response.getSource().get(field)));
          }
        }
        return Status.OK;
      } else {
        return Status.NOT_FOUND;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    try {
      final GetResponse response = client.prepareGet(indexKey, table, key).execute().actionGet();

      if (response.isExists()) {
        for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
          response.getSource().put(entry.getKey(), entry.getValue());
        }

        client.prepareIndex(indexKey, table, key).setSource(response.getSource()).execute().actionGet();

        return Status.OK;
      } else {
        return Status.NOT_FOUND;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(
      String table,
      String startkey,
      int recordcount,
      Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    try{
      final SearchResponse response = client.prepareSearch(indexKey)
          .setTypes(table)
          .setQuery(QueryBuilders.matchQuery(
            "_id", startkey + ' '
            + startkey.substring(0, Math.min(5, startkey.length()))
          ))
          .setSize(recordcount)
          .execute()
          .actionGet();

      HashMap<String, ByteIterator> entry;

      for (SearchHit hit : response.getHits()) {
        entry = new HashMap<>(fields.size());
        for (String field : fields) {
          entry.put(field, new StringByteIterator((String) hit.getSource().get(field)));
        }
        result.add(entry);
      }

      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }
}
