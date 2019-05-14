package com.troy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

/**
 * @description:
 * @author: ShadowMo
 * @createtime: 2019-05-10 15:23
 */

/**
 * 有关如何向tsdb添加点的示例。
 */
public class putDataExample {
    private static String pathToConfigFile;

    public static void processArgs(final String[] args) {
        // 将这些设置为参数，这样您就不必保留路径信息
        // 源文件
        if (args != null && args.length > 0) {
            pathToConfigFile = args[0];
        }
    }

    public static void main(final String[] args) throws Exception {
        processArgs(args);
        //创建一个配置对象，其中包含用于解析的文件路径。或者手动
        //覆盖设置。
        // e.g. config.overrideConfig("tsd.storage.hbase.zk_quorum", "localhost");
        final Config config;
        if (pathToConfigFile != null && !pathToConfigFile.isEmpty()) {
            config = new Config(pathToConfigFile);
        } else {
            //从/etc/opentsdb/opentsdb.conf等搜索默认配置
            config = new Config(true);
        }
        final TSDB tsdb = new TSDB(config);

        // Declare new metric
        String metricName = "tsdb.test.sysinfo1";
        // First check to see it doesn't already exist
        // we don't actually need this for the first
        byte[] byteMetricUID;
        // .addPoint() call below.
        // TODO: Ideally we could just call a not-yet-implemented tsdb.uIdExists()
        // function.
        // Note, however, that this is optional. If auto metric is enabled
        // (tsd.core.auto_create_metrics), the UID will be assigned in call to
        // addPoint().
        try {
            byteMetricUID = tsdb.getUID(UniqueIdType.METRIC, metricName);
        } catch (IllegalArgumentException iae) {
            System.out.println("Metric name not valid.");
            iae.printStackTrace();
            System.exit(1);
        } catch (NoSuchUniqueName nsune) {
            // If not, great. Create it.
            byteMetricUID = tsdb.assignUid("metric", metricName);
        }

        // Make a single datum
        long timestamp = System.currentTimeMillis() / 1000;
        long value = 1314;
        // Make key-val
        Map<String, String> tags = new HashMap<String, String>(1);
        tags.put("tags", "test");
        // Start timer
        long startTime1 = System.currentTimeMillis();

        //每隔3秒写一些数据点。每次写都会
        //返回一个延迟的（类似于Java Future或JS Promise）
        //成功时调用“null”值或者调用
        // exception.
        int n = 100;
        ArrayList<Deferred<Object>> deferreds = new ArrayList<Deferred<Object>>(n);
        for (int i = 0; i < n; i++) {
            Deferred<Object> deferred = tsdb.addPoint(metricName, timestamp, value + i, tags);
            deferreds.add(deferred);
            timestamp += 3;//没3s新增一个数据点
        }

        // Add the callbacks to the deferred object. (They might have already
        // returned, btw)
        // This will cause the calling thread to wait until the add has completed.
        System.out.println("Waiting for deferred result to return...");
        Deferred.groupInOrder(deferreds)
                .addErrback(new putDataExample().new errBack())
                .addCallback(new putDataExample().new succBack())
                // Block the thread until the deferred returns it's result.
                .join();
        // Alternatively you can add another callback here or use a join with a
        // timeout argument.

        // End timer.
        long elapsedTime1 = System.currentTimeMillis() - startTime1;
        System.out.println("\nAdding " + n + " points took: " + elapsedTime1
                + " milliseconds.\n");

        // Gracefully shutdown connection to TSDB. This is CRITICAL as it will
        // flush any pending operations to HBase.
        tsdb.shutdown().join();
    }

    // This is an optional errorback to handle when there is a failure.
    class errBack implements Callback<String, Exception> {
        @Override
        public String call(final Exception e) throws Exception {
            String message = ">>>>>>>>>>>Failure!>>>>>>>>>>>";
            System.err.println(message + " " + e.getMessage());
            e.printStackTrace();
            return message;
        }
    }


    // This is an optional success callback to handle when there is a success.
    class succBack implements Callback<Object, ArrayList<Object>> {
        @Override
        public Object call(final ArrayList<Object> results) {
            System.out.println("Successfully wrote " + results.size() + " data points");
            return null;
        }
    }

}

