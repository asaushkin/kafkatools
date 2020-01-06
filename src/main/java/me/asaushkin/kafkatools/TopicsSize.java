package me.asaushkin.kafkatools;

import org.apache.commons.io.IOUtils;
import org.json.*;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TopicsSize {
    public static void main(String[] args) throws IOException {

        InputStream is = new FileInputStream(args[0]);
        String jsonTxt = IOUtils.toString(new BufferedInputStream(is),
                Charset.defaultCharset());

        Pattern r = Pattern.compile("^(.*)-(\\d{1,})$");

        JSONObject obj = new JSONObject(jsonTxt);

        JSONArray brokers = obj.getJSONArray("brokers");

        System.out.println("-- table structure\n\n" +
                "drop table if exists partitions cascade;\n" +
                "drop table if exists logdirs;\n" +
                "drop table if exists brokers;\n\n" +
                "" +
                "create table brokers (id int primary key);\n\n" +
                "create table logdirs (path text not null, broker_id \n" +
                "  int not null references brokers(id), primary key (path, broker_id));\n\n" +
                "create table partitions (broker_id int not null, partition_path text not null, \n" +
                "  name text not null, num int not null, size bigint not null, \n" +
                "  offset_lag int not null, is_future boolean not null);\n\n" +
                "" +
                "create or replace view broker_topics as\n" +
                "select * from (\n" +
                "    select\n" +
                "           name,\n" +
                "           broker_id,\n" +
                "           (sum(size) / 1024 / 1024 / 1024)::int as size\n" +
                "    from partitions\n" +
                "    group by name, broker_id\n" +
                ") as p\n" +
                "order by size desc;\n\n" +
                "" +
                "-- data\n\n");

        for (int i = 0; i < brokers.length(); i++)
        {
            int brokerId = brokers.getJSONObject(i).getInt("broker");
            System.out.println(String.format("insert into brokers (id) values (%d);", brokerId));

            JSONArray logDirs = brokers.getJSONObject(i).getJSONArray("logDirs");
            for (int j = 0; j < logDirs.length(); j++)
            {
                String logDir = logDirs.getJSONObject(j).getString("logDir");
                System.out.println(String.format("insert into logdirs (path, broker_id) values ('%s', %d);", logDir, brokerId));

                JSONArray partitions = logDirs.getJSONObject(j).getJSONArray("partitions");
                for (int k = 0; k < partitions.length(); k++)
                {
                    String partition = partitions.getJSONObject(k).getString("partition");
                    BigInteger size = partitions.getJSONObject(k).getBigInteger("size");
                    int offsetLag = partitions.getJSONObject(k).getInt("offsetLag");
                    Boolean isFuture = partitions.getJSONObject(k).getBoolean("isFuture");
                    Matcher m = r.matcher(partition);
                    if (m.find( )) {
                        System.out.println(String.format("insert into partitions (broker_id, partition_path, name, num," +
                                        "size, offset_lag, is_future) values (%d, '%s', '%s', %s, %s, %d, %s);",
                                brokerId, logDir, m.group(1), m.group(2), size.toString(), offsetLag, isFuture.toString()));
                    } else {
                        throw new RuntimeException("No pattern for partition line");
                    }
                }
            }
        }
    }
}
