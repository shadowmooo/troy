package com.troy;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

/**
 * @description:
 * @author: ShadowMo
 * @createtime: 2019-05-06 16:41
 */
public class TSDBTest {

    public static void main(String[] args) throws IOException {

        putData();
    }

    public static void putData() throws IOException {

        Config config = new Config(true);
        TSDB tsdb = new TSDB(config);

        String metricName = "tsdb.test.metric1";
        byte[] byteMetricUID;
        try {
            byteMetricUID = tsdb.getUID(UniqueId.UniqueIdType.METRIC, metricName);
        } catch (IllegalArgumentException iae) {
            System.out.println("Metric name not valid.");
            iae.printStackTrace();
            System.exit(1);
        } catch (NoSuchUniqueName nsune) {
            // If not, great. Create it.
            byteMetricUID = tsdb.assignUid("metric", metricName);
        }

        Map<String, String> tags = new HashMap<String, String>(1);
        tags.put("script", "example1");

        long timestamp = System.currentTimeMillis() / 1000;
        long value = 300000;
        long startTime1 = System.currentTimeMillis();
        int n = 100;
        ArrayList<Deferred<Object>> deferreds = new ArrayList<Deferred<Object>>(n);
        for (int i = 0; i < n; i++) {
            Deferred<Object> deferred = tsdb.addPoint(metricName, timestamp, value + i, tags);
            deferreds.add(deferred);
            timestamp += 30;
        }
        System.out.println("Waiting for deferred result to return...");
    }

    public static void getData() throws IOException {

        Config config = new Config(true);
        //config.overrideConfig("tsd.storage.hbase.zk_quorum", "node2.troy.com,node3.troy.com,node4.troy.com");
        final TSDB tsdb = new TSDB(config);

        // main query
        final TSQuery query = new TSQuery();

        query.setStart("5d-ago");

        final TSSubQuery subQuery = new TSSubQuery();
        subQuery.setMetric("sys.cpu.node1");

        // filters are optional but useful.
        final List<TagVFilter> filters = new ArrayList<TagVFilter>(1);
        filters.add(new TagVFilter.Builder()
                .setType("regexp")
                .setFilter("target")
                .setTagk("tag")
                .setGroupBy(true)
                .build());
        subQuery.setFilters(filters);

        subQuery.setAggregator("sum");

        final ArrayList<TSSubQuery> subQueries = new ArrayList<TSSubQuery>(1);
        subQueries.add(subQuery);
        query.setQueries(subQueries);
        query.setMsResolution(true); // otherwise we aggregate on the second.
        query.validateAndSetQuery();

        Query[] tsdbqueries = query.buildQueries(tsdb);

        final int nqueries = tsdbqueries.length;
        final ArrayList<DataPoints[]> results = new ArrayList<DataPoints[]>(
                nqueries);
        final ArrayList<Deferred<DataPoints[]>> deferreds =
                new ArrayList<Deferred<DataPoints[]>>(nqueries);
        for (int i = 0; i < nqueries; i++) {
            deferreds.add(tsdbqueries[i].runAsync());
        }

        long startTime = DateTime.nanoTime();

        class QueriesCB implements Callback<Object, ArrayList<DataPoints[]>> {
            public Object call(final ArrayList<DataPoints[]> queryResults)
                    throws Exception {
                results.addAll(queryResults);
                return null;
            }
        }

        class QueriesEB implements Callback<Object, Exception> {
            @Override
            public Object call(final Exception e) throws Exception {
                System.err.println("Queries failed");
                e.printStackTrace();
                return null;
            }
        }

        try {
            Deferred.groupInOrder(deferreds)
                    .addCallback(new QueriesCB())
                    .addErrback(new QueriesEB())
                    .join();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // End timer.
        double elapsedTime = DateTime.msFromNanoDiff(DateTime.nanoTime(), startTime);
        System.out.println("Query returned in: " + elapsedTime + " milliseconds.");

        // now all of the results are in so we just iterate over each set of
        // results and do any processing necessary.
        for (final DataPoints[] dataSets : results) {
            for (final DataPoints data : dataSets) {
                System.out.print(data.metricName());
                Map<String, String> resolvedTags = data.getTags();
                for (final Map.Entry<String, String> pair : resolvedTags.entrySet()) {
                    System.out.print(" " + pair.getKey() + "=" + pair.getValue());
                }
                System.out.print("\n");

                final SeekableView it = data.iterator();
                while (it.hasNext()) {
                    final DataPoint dp = it.next();
                    System.out.println("  " + dp.timestamp() + " "
                            + (dp.isInteger() ? dp.longValue() : dp.doubleValue()));
                }
                System.out.println("");
            }
        }

        // Gracefully shutdown connection to TSDB
        try {
            tsdb.shutdown().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
