package edu.rit.ibd.a6;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.TreeMap;

public class GenerateLOne {

    public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColTrans = args[2];
		final String mongoColL1 = args[3];
		final int minSup = Integer.valueOf(args[4]);


       /* final String mongoDBURL = "None";
        final String mongoDBName = "Ass6_2";
        final String mongoColTrans = "Transactions";
        final String mongoColL1 = "L1_0_5";
        final int minSup = 5;*/

        MongoClient client = getClient(mongoDBURL);
        MongoDatabase db = client.getDatabase(mongoDBName);

        MongoCollection<Document> transactions = db.getCollection(mongoColTrans);
        MongoCollection<Document> l1 = db.getCollection(mongoColL1);

        // TODO Your code here!

        /*
         *
         * Extract single items from the transactions. Only single items that are present in at least minSup transactions should survive.
         *
         * You need to compose the new documents to be inserted in the L1 collection as {_id: {pos_0:iid}, count:z}.
         *
         */

        // You can implement this "by hand" using Java, an aggregation query, or a mix.
        // Be mindful of main memory and use batchSize when you request documents from MongoDB.
        TreeMap<Integer, Integer> treeMap = new TreeMap<>();
        for (Document document : transactions.find().batchSize(100)) {
            for (int iid : document.getList("items", Integer.class)) {
                treeMap.put(iid, treeMap.getOrDefault(iid, 0) + 1);
            }
        }
        for (int iid : treeMap.keySet()) {
            int count = treeMap.get(iid);
            if (count >= minSup) {
                l1.insertOne(new Document()
                        .append("count", count)
                        .append("items", new Document("pos_0", iid)));
            }
        }
        client.close();
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
