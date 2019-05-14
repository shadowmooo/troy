package com.troy;

import com.troy.util.DateUtil;
import org.junit.Test;
import org.opentsdb.client.request.Filter;

import org.opentsdb.client.util.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class OpentsdbTestTest {
    private static Logger log = LoggerFactory.getLogger(OpentsdbTestTest.class);

    @Test
    public void putData() {
        OpentsdbTest opentsdbTest = new OpentsdbTest("http://10.0.202.49:4242");

        Map<String, String> tagMap = new HashMap<String, String>();
        tagMap.put("chl", "hqdApp");
        try {
            opentsdbTest.putData("metric-t", DateUtil.parseStrToDate("20190501 12:05", "yyyyMMdd HH:mm"), (long) 2010, tagMap);
            opentsdbTest.putData("metric-t", DateUtil.parseStrToDate("20190502 12:06", "yyyyMMdd HH:mm"), (long) 2301, tagMap);
            opentsdbTest.putData("metric-t", DateUtil.parseStrToDate("20190504 12:07", "yyyyMMdd HH:mm"), (long) 2242, tagMap);
            opentsdbTest.putData("metric-t", DateUtil.parseStrToDate("20190506 12:08", "yyyyMMdd HH:mm"), (long) 2005, tagMap);
            opentsdbTest.putData("metric-t", DateUtil.parseStrToDate("20190507 12:09", "yyyyMMdd HH:mm"), (long) 2603, tagMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void putData1() {
        OpentsdbTest opentsdbTest = new OpentsdbTest("http://10.0.202.49:4242");
        Map<String, String> tagMap = new HashMap<String, String>();
        tagMap.put("tag", "target");
        try {
            while (true) {
                opentsdbTest.putData("sys.cpu.node1", new Date(), (long) new Random().nextInt(500), tagMap);
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void putData2() {
        OpentsdbTest client = new OpentsdbTest("");
        try {
            Map<String, String> tagMap = new HashMap<String, String>();
            tagMap.put("host", "192.168.100.200");

            client.putData("anysense-alarm", DateUtil.parseStrToDate("20160627 12:15", "yyyyMMdd HH:mm"), 210l, tagMap);


        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

    @Test
    public void putData3() {
    }

    @Test
    public void getData1() {
        OpentsdbTest client = new OpentsdbTest("http://10.0.202.49:4242");
        try {
            Filter filter = new Filter();
            filter.setType("regexp");
            filter.setTagk("chl");
            filter.setFilter("hqdApp");
            filter.setGroupBy(Boolean.TRUE);
            String resContent = client.getData("metric-t", filter, Aggregator.avg.name(), "1h",
                    "2019-05-01 12:00:00", "2019-05-10 13:00:00");

            log.info(">>>" + resContent);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getData2() {
        OpentsdbTest client = new OpentsdbTest("http://10.0.202.49:4242");
        try {
            Filter filter = new Filter();
            String tagk = "tag";
            String tagvFtype = OpentsdbTest.FILTER_TYPE_WILDCARD;
            String tagvFilter = "target*";

            Map<String, Map<String, Object>> tagsValuesMap = client.getData("sys.cpu.node1", tagk, tagvFtype, tagvFilter, Aggregator.avg.name(), "1s",
                    "2019-05-01 12:00:00", "2019-05-19 12:00:00", "yyyyMMdd HHmmss");

            for (Iterator<String> it = tagsValuesMap.keySet().iterator(); it.hasNext(); ) {
                String tags = it.next();
                System.out.println((">> tags: " + tags));
                Map<String, Object> tvMap = tagsValuesMap.get(tags);
                for (Iterator<String> it2 = tvMap.keySet().iterator(); it2.hasNext(); ) {
                    String time = it2.next();
                    System.out.println(("    >> " + time + " <-> " + tvMap.get(time)));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getData3() {
    }

    @Test
    public void convertContentToMap() {
    }
}