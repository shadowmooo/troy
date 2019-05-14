package com.troy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

/**
 * @description:
 * @author: ShadowMo
 * @createtime: 2019-05-10 15:27
 */
public class getDataExample {

    public static void main(final String[] args) throws IOException {

        //将这些设置为参数，这样您就不必保留路径信息
        //源文件
        String pathToConfigFile = (args != null && args.length > 0 ? args[0] : null);

        //创建一个配置对象，其中包含用于解析的文件路径。或者手动覆盖设置。
        //例如config.overrideConfig（“tsd.storage.hbase.zk_quorum”，“localhost”）;
        final Config config;
        if (pathToConfigFile != null && !pathToConfigFile.isEmpty()) {
            config = new Config(pathToConfigFile);
        } else {
            //从/etc/opentsdb/opentsdb.conf等搜索默认配置
            config = new Config(true);
        }

        final TSDB tsdb = new TSDB(config);

        final TSQuery query = new TSQuery();
        // 设置开始时间
        // http://opentsdb.net/docs/build/html/user_guide/query/dates.html
        query.setStart("10m-ago");
        // 可选：设置其他全局查询参数
        // 至少需要一个子查询。您可以在此处指定metrics和tags
        final TSSubQuery subQuery = new TSSubQuery();
        subQuery.setMetric("tsdb.test.sysinfo1");
        // 设置过滤器
        final List<TagVFilter> filters = new ArrayList<TagVFilter>(1);
        filters.add(new TagVFilter.Builder()
                .setType("literal_or")
                .setFilter("test")
                .setTagk("tags")
                .setGroupBy(true)
                .build());
        subQuery.setFilters(filters);

        // 你必须设置一个聚合器。只需将名称作为字符串提供
        subQuery.setAggregator("sum");

        // 重要提示：不要忘记添加子查询
        final ArrayList<TSSubQuery> subQueries = new ArrayList<TSSubQuery>(1);
        subQueries.add(subQuery);
        query.setQueries(subQueries);
        query.setMsResolution(true); // 否则我们在第二个聚合。

        // 确保查询有效。如果出现问题，这将抛出异常
        query.validateAndSetQuery();

        // compile the queries into TsdbQuery objects behind the scenes
        Query[] tsdbqueries = query.buildQueries(tsdb);

        // 创建一些数组来存储结果和异步调用
        final int nqueries = tsdbqueries.length;
        final ArrayList<DataPoints[]> results = new ArrayList<DataPoints[]>(nqueries);
        final ArrayList<Deferred<DataPoints[]>> deferreds = new ArrayList<Deferred<DataPoints[]>>(nqueries);

        // 这将异步执行每个子查询并放入
        // 在数组中延迟，以便我们可以等待它们完成。
        for (int i = 0; i < nqueries; i++) {
            deferreds.add(tsdbqueries[i].runAsync());
        }

        // Start timer
        long startTime = DateTime.nanoTime();

        // 这是一个必需的回调类，用于在每个之后存储结果
        // 查询已完成
        class QueriesCB implements Callback<Object, ArrayList<DataPoints[]>> {
            @Override
            public Object call(final ArrayList<DataPoints[]> queryResults)
                    throws Exception {
                results.addAll(queryResults);
                return null;
            }
        }

        // 确保处理可能出现的任何错误
        class QueriesEB implements Callback<Object, Exception> {
            @Override
            public Object call(final Exception e) throws Exception {
                System.err.println("Queries failed");
                e.printStackTrace();
                return null;
            }
        }

        // 这将导致调用线程等到所有查询
        // 已完成
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
                /*
                 *关于SeekableView的重点：
                 *因为在迭代期间没有复制数据而没有新对象
                 *创建后，返回的DataPoint不得存储和获取
                 *一旦在迭代器上调用next，就会失效（实际上它
                 *不会失效，而是内容发生变化）。如果你想
                 *要存储单个数据点，您需要复制时间戳和
                 *将每个DataPoint的值放入您自己的数据结构中。
                 *
                 *在绝大多数情况下，迭代器将用于一次
                 *通过所有数据点，这就是为什么它不是一个问题，如果
                 *迭代器就像一个瞬态“视图”。迭代将非常
                 *便宜，因为不需要内存分配（实例化除外）
                 *开头的实际迭代器）。
                 */
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

