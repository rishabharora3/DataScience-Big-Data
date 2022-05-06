package edu.rit.ibd.a6;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.TreeMap;

public class GenerateLOneOpt {

    public static void main(String[] args) throws Exception {
        final String mongoDBURL = args[0];
        final String mongoDBName = args[1];
        final String mongoColTrans = args[2];
        final String mongoColL1 = args[3];
        final int minSup = Integer.valueOf(args[4]);

        MongoClient client = getClient(mongoDBURL);
        MongoDatabase db = client.getDatabase(mongoDBName);

        MongoCollection<Document> transactions = db.getCollection(mongoColTrans);
        MongoCollection<Document> l1 = db.getCollection(mongoColL1);

        // TODO Your code here!

        /*
         *
         * Extract single items from the transactions. Only single items that are present in at least minSup transactions should survive.
         *
         * Keep track of the transactions associated to each item using an array field named 'transactions'. Also, use _ids such that
         * 	they reflect the lexicographical order in which documents are processed.
         *
         */
        TreeMap<Integer, Integer> treeMap = new TreeMap<>();
        TreeMap<Integer, ArrayList<Integer>> treeMapIds = new TreeMap<>();
        for (Document document : transactions.find().batchSize(100)) {
            for (int iid : document.getList("items", Integer.class)) {
                treeMap.put(iid, treeMap.getOrDefault(iid, 0) + 1);
                ArrayList<Integer> list = treeMapIds.getOrDefault(iid, new ArrayList<>());
                list.add(document.getInteger("_id"));
                Collections.sort(list);
                treeMapIds.put(iid, list);
            }
        }
        int id = 0;
        for (int iid : treeMap.keySet()) {
            int count = treeMap.get(iid);
            if (count >= minSup) {
                l1.insertOne(new Document()
                        .append("_id", id)
                        .append("count", count)
                        .append("items", new Document("pos_0", iid))
                        .append("transactions", treeMapIds.get(iid)));
                id++;
            }
        }
        // TODO End of your code!

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
