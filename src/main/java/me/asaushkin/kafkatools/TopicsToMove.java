package me.asaushkin.kafkatools;

import java.sql.*;

public class TopicsToMove {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:postgresql://127.0.0.1/ags?user=ags&password=secret&ssl=false";
        Connection conn = DriverManager.getConnection(url);

        PreparedStatement ps
                = conn.prepareStatement("select * from broker_topics " +
                "where broker_id = 1 and name like '%user%' order by name");
        ResultSet rs = ps.executeQuery();

        StringBuffer f = new StringBuffer();
        f.append("{\n" +
                "  \"topics\": [\n");

        int i = 0;
        while (rs.next()) {
            String s = rs.getString(1);

            if (i > 0) {
                f.append(",\n");
            }

            f.append(String.format("    {\n" +
                    "      \"topic\": \"%s\"\n" +
                    "    }", s));
            i++;
        }

        f.append("\n  ],\n" +
                "  \"version\": 1\n" +
                "}\n");

        System.out.println(f);
    }
}
