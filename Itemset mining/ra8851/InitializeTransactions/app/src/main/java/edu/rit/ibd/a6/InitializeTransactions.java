package edu.rit.ibd.a6;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class InitializeTransactions {

    public static void main(String[] args) throws Exception {
        final String jdbcURL = args[0];
        final String jdbcUser = args[1];
        final String jdbcPwd = args[2];
        final String sqlQuery = args[3];
        final String mongoDBURL = args[4];
        final String mongoDBName = args[5];
        final String mongoCol = args[6];


       /* final String jdbcURL = "jdbc:mysql://localhost:3306/imdbNew?useCursorFetch=true";
        final String jdbcUser = "root";
        final String jdbcPwd = "password";
        final String sqlQuery = "SELECT mid AS tid, pid AS iid FROM Actor JOIN Movie ON id=mid WHERE year BETWEEN 1990 AND 2010 AND totalvotes > 1000 AND mid IN (SELECT mid FROM MovieGenre JOIN Genre ON id=gid WHERE name LIKE '%c%') UNION SELECT mid AS tid, pid AS iid FROM Director JOIN Movie ON id=mid WHERE year BETWEEN 1990 AND 2010 AND totalvotes > 1000 AND mid IN (SELECT mid FROM MovieGenre JOIN Genre ON id=gid WHERE name LIKE '%c%') UNION SELECT mid AS tid, pid AS iid FROM Producer JOIN Movie ON id=mid WHERE year BETWEEN 1990 AND 2010 AND totalvotes > 1000 AND mid IN (SELECT mid FROM MovieGenre JOIN Genre ON id=gid WHERE name LIKE '%c%') UNION SELECT mid AS tid, pid AS iid FROM Writer JOIN Movie ON id=mid WHERE year BETWEEN 1990 AND 2010 AND totalvotes > 1000 AND mid IN (SELECT mid FROM MovieGenre JOIN Genre ON id=gid WHERE name LIKE '%c%') ORDER BY tid, iid";
        final String mongoDBURL = "None";
        final String mongoDBName = "Assignment6";
        final String mongoCol = "Transactions_0";*/

        Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);

        MongoClient client = getClient(mongoDBURL);
        MongoDatabase db = client.getDatabase(mongoDBName);

        MongoCollection<Document> transactions = db.getCollection(mongoCol);

        // TODO Your code here!!!

        /*
         *
         * Run the input SQL query over the input URL. Remember to use the fetch size to only retrieve a certain number of tuples at a time (useCursorFetch=true will
         * 	be part of the URL).
         *
         * For each transaction (tid), you need to create a new document and store it in the MongoDB collection specified as input. Such document must contain an array
         * 	in which the elements are iid lexicographically sorted.
         *
         */

        // Run the SQL query to retrieve the data. Recall that it contains two attributes iid and tid, and it is always sorted by tid and, then, iid.
        //	Be mindful of main memory and use an appropriate batch size.


        PreparedStatement st = con.prepareStatement(sqlQuery);
        st.setFetchSize(100);
        ResultSet rs = st.executeQuery();
        while (rs.next()) {
            int tid = rs.getInt("tid");
            int iid = rs.getInt("iid");
            FindIterable<Document> iterDoc = transactions.find(Filters.eq("_id", tid));
            Document document = iterDoc.first();
            List<Integer> items;
            if (document != null) {
                items = document.getList("items", Integer.class);
                items.add(iid);
                document.append("items", items);
                transactions.updateOne(Filters.eq("_id", tid),
                        new Document().append("$set", document));
            } else {
                items = new ArrayList<>();
                items.add(iid);
                transactions.insertOne(new Document()
                        .append("_id", tid)
                        .append("items", items));
            }
        }

        // TODO End of your code!

        client.close();
        con.close();
    }

    private static MongoClient getClient(String mongoDBURL) {
        MongoClient client = null;
        if (mongoDBURL.equals("None"))
            client = new MongoClient();
        else
            client = new MongoClient(new MongoClientURI(mongoDBURL));
        return client;
    }

}
