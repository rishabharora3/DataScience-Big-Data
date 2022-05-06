package edu.rit.ibd.a6;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.client.FindIterable;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class GenerateLK {

    public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColTrans = args[2];
		final String mongoColCK = args[3];
		final String mongoColLK = args[4];
		final int minSup = Integer.valueOf(args[5]);

        /*final String mongoDBURL = "None";
        final String mongoDBName = "Ass6_2";
        final String mongoColTrans = "Transactions_1";
        final String mongoColCK = "C2_1_2";
        final String mongoColLK = "L2_1_2";
        final int minSup = 2;*/

        MongoClient client = getClient(mongoDBURL);
        MongoDatabase db = client.getDatabase(mongoDBName);

        MongoCollection<Document> transactions = db.getCollection(mongoColTrans);
        MongoCollection<Document> ck = db.getCollection(mongoColCK);
        MongoCollection<Document> lk = db.getCollection(mongoColLK);

        // TODO Your code here!

        /*
         *
         * For each transaction t, check whether the items of a document c in ck are contained in the items of t. If so, increment by one the count of c.
         *
         * All the documents in ck that meet the minimum support will be copied to lk.
         *
         * You can use $inc to update the count of a document.
         *
         * Alternatively, you can also copy all documents in ck to lk first and, then, perform the previous computations.
         *
         */

        // You must figure out the value of k.

        // For each document in Ck, check the items are present in the transactions at least minSup times.

        // You can implement this "by hand" using Java, an aggregation query, or a mix.
        FindIterable<Document> ckItr = ck.find().batchSize(100);
        FindIterable<Document> transactionsItr = transactions.find().batchSize(100);
        for (Document document : ckItr) {
            List<Integer> ckItems = getList((Document) document.get("items"));
            int count = checkIfItemsInT(ckItems, transactionsItr);
            if (count >= minSup) {
                addToLk(ckItems, lk, count);
            }
        }
        // TODO End of your code here!
        client.close();
    }

    private static void addToLk(List<Integer> ckItems, MongoCollection<Document> lk, int count) {
        lk.insertOne(new Document()
                .append("count", count)
                .append("items", getLkDoc(ckItems)));
    }

    private static Document getLkDoc(List<Integer> ckItems) {
        Document document = new Document();
        for (int i = 0; i < ckItems.size(); i++) {
            document.append("pos_" + i, ckItems.get(i));
        }
        return document;
    }

    private static int checkIfItemsInT(List<Integer> ckItems, FindIterable<Document> transactionsItr) {
        int c = 0;
        for (Document document : transactionsItr) {
            List<Integer> list = document.getList("items", Integer.class);
            if (list.containsAll(ckItems))
                c++;
        }
        return c;
    }

    private static ArrayList<Integer> getList(Document items) {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            list.add(items.getInteger("pos_" + i));
        }
        return list;
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
