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

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
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
  private Client client;
  private String indexKey;
  /**
   *
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    final Properties props = getProperties();

    final String pathHome = props.getProperty("path.home");

    this.indexKey = props.getProperty("es.index.key", DEFAULT_INDEX_KEY);

    int numberOfShards = parseIntegerProperty(props, "es.number_of_shards", NUMBER_OF_SHARDS);
    int numberOfReplicas = parseIntegerProperty(props, "es.number_of_replicas", NUMBER_OF_REPLICAS);

    Boolean newdb = Boolean.parseBoolean(props.getProperty("es.newdb", "false"));
    Builder settings = Settings.builder().put("cluster.name", DEFAULT_CLUSTER_NAME);
    if (pathHome != null) {
      settings.put("path.home", pathHome);
    }

    // if properties file contains elasticsearch user defined properties
    // add it to the settings file (will overwrite the defaults).
    for (final Entry<Object, Object> e : props.entrySet()) {
      if (e.getKey() instanceof String) {
        final String key = (String) e.getKey();
        if (key.startsWith("es.setting.")) {
          settings.put(key.substring("es.setting.".length()), e.getValue());
        }
      }
    }
    final String clusterName = settings.get("cluster.name");
    System.err.println("Elasticsearch starting node = " + clusterName);
    System.err.println("Elasticsearch node path.home = " + settings.get("path.home"));

    settings.put("client.transport.sniff", true)
            .put("client.transport.ignore_cluster_name", false)
            .put("client.transport.ping_timeout", "30s")
            .put("client.transport.nodes_sampler_interval", "30s");
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

    final boolean exists =
        client.admin().indices()
            .exists(Requests.indicesExistsRequest(indexKey)).actionGet()
            .isExists();
    if (exists && newdb) {
      client.admin().indices().prepareDelete(indexKey).get();
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

  private int parseIntegerProperty(final Properties properties, final String key, final int defaultValue) {
    final String value = properties.getProperty(key);
    return value == null ? defaultValue : Integer.parseInt(value);
  }

  @Override
  public void cleanup() throws DBException {
    if (client != null) {
      client.close();
      client = null;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      final XContentBuilder doc = jsonBuilder();

      doc.startObject();
      for (final Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        doc.field(entry.getKey(), entry.getValue());
      }

      doc.field("key", key);
      doc.endObject();

      client.prepareIndex(indexKey, table).setSource(doc).execute().actionGet();

      return Status.OK;
    } catch (final Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      final SearchResponse searchResponse = search(key);
      if (searchResponse.getHits().totalHits == 0) {
        return Status.NOT_FOUND;
      }

      final String id = searchResponse.getHits().getAt(0).getId();

      final DeleteResponse deleteResponse = client.prepareDelete(indexKey, table, id).execute().actionGet();
      if (deleteResponse.status().equals(RestStatus.NOT_FOUND)) {
        return Status.NOT_FOUND;
      } else {
        return Status.OK;
      }
    } catch (final Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status read(
          final String table,
          final String key,
          final Set<String> fields,
          final Map<String, ByteIterator> result) {
    try {
      final SearchResponse searchResponse = search(key);
      if (searchResponse.getHits().totalHits == 0) {
        return Status.NOT_FOUND;
      }

      final SearchHit hit = searchResponse.getHits().getAt(0);
      if (fields != null) {
        for (String field : fields) {
          result.put(field, new StringByteIterator(
                  (String) hit.getField(field).getValue()));
        }
      } else {
        for (final Map.Entry<String, SearchHitField> e : hit.getFields().entrySet()) {
          result.put(e.getKey(), new StringByteIterator((String) e.getValue().getValue()));
        }
      }
      return Status.OK;

    } catch (final Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      final SearchResponse response = search(key);
      if (response.getHits().totalHits == 0) {
        return Status.NOT_FOUND;
      }

      final SearchHit hit = response.getHits().getAt(0);
      for (final Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        hit.getSource().put(entry.getKey(), entry.getValue());
      }

      client.prepareIndex(indexKey, table, key).setSource(hit.getSource()).get();

      return Status.OK;

    } catch (final Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(
      final String table,
      final String startkey,
      final int recordcount,
      final Set<String> fields,
      final Vector<HashMap<String, ByteIterator>> result) {
    try {
      final RangeQueryBuilder query = new RangeQueryBuilder("key").gte(startkey);
      final SearchResponse response = client.prepareSearch(indexKey).setQuery(query).setSize(recordcount).get();

      for (final SearchHit hit : response.getHits()) {
        final HashMap<String, ByteIterator> entry;
        if (fields != null) {
          entry = new HashMap<>(fields.size());
          for (final String field : fields) {
            entry.put(field, new StringByteIterator((String) hit.getSource().get(field)));
          }
        } else {
          entry = new HashMap<>(hit.getFields().size());
          for (final Map.Entry<String, SearchHitField> field : hit.getFields().entrySet()) {
            entry.put(field.getKey(), new StringByteIterator((String) field.getValue().getValue()));
          }
        }
        result.add(entry);
      }
      return Status.OK;
    } catch (final Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }


  private SearchResponse search(final String key) {
    return client.prepareSearch(indexKey).setQuery(new TermQueryBuilder("key", key)).get();
  }

}