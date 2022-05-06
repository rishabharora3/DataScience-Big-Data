package edu.rit.ibd.a6;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AprioriGenOpt {
    private static int id = 0;

    public static void main(String[] args) throws Exception {
        final String mongoDBURL = args[0];
        final String mongoDBName = args[1];
        final String mongoColLKMinusOne = args[2];
        final String mongoColLK = args[3];
        final int minSup = Integer.valueOf(args[4]);

        MongoClient client = getClient(mongoDBURL);
        MongoDatabase db = client.getDatabase(mongoDBName);

        MongoCollection<Document> lKMinusOne = db.getCollection(mongoColLKMinusOne);
        MongoCollection<Document> lk = db.getCollection(mongoColLK);

        // TODO Your code here!

        /*
         *
         * The documents include the transactions that contain them, so if a new document is added to CK, we can directly compute its transactions by performing the intersection. Having the actual
         * 	transactions entails that we also know its number, so we can discard those that do not meet the minimum support. Items can be processed in ascending order.
         *
         */

        // You must figure out the value of k - 1.

        // You can implement this "by hand" using Java, an aggregation query, or a mix.

        // Remember that there is a single join step. The prune step is not used anymore.

        // Make sure the _ids of the documents are according to the lexicographical order of the items. You can start joining documents
        //	whose _ids are strictly greater than the current document. Also, the first time a pair of documents do not join, we can safely stop.

        // Both documents contain the arrays of transactions lexicographically sorted. The new document will have the intersecion of both sets
        //	of transactions.

       /* int kMinusOne = 0;
        Document document = lKMinusOne.find().first();
        if (document != null)
            kMinusOne = ((Document) document.get("items")).size();*/
        FindIterable<Document> pIter = lKMinusOne.find().batchSize(100);
        FindIterable<Document> qIter = lKMinusOne.find().batchSize(100);
//        ArrayList<Document> lkMinusOneList = new ArrayList<>();
//        lKMinusOne.find().batchSize(100).into(lkMinusOneList);

        for (Document docP : pIter) {
            List<Integer> p = getList((Document) docP.get("items"));
            for (Document docQ : qIter) {
                List<Integer> q = getList((Document) docQ.get("items"));
                if (p.equals(q))
                    continue;
                for (int i = 0; i < p.size(); i++) {
                    if (i < p.size() - 1 && !p.get(i).equals(q.get(i))) {
                        break;
                    } else if (i == p.size() - 1 && p.get(i) < q.get(i)) {
                        List<Integer> lkDoc = new ArrayList<>(p);
                        lkDoc.add(q.get(i));
                        List<Integer> transactionList = docP.getList("transactions", Integer.class);
                        transactionList.retainAll(docQ.getList("transactions", Integer.class));
                        int count = transactionList.size();
                        if (count >= minSup) {
                            addToLk(lkDoc, lk, count, transactionList);
                        }
                    }
                }
            }
        }
        // TODO End of your code!
        client.close();
    }

    private static ArrayList<Integer> getList(Document items) {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            list.add(items.getInteger("pos_" + i));
        }
        return list;
    }

    private static void addToLk(List<Integer> lkDoc, MongoCollection<Document> ck, int count, List<Integer> transactionList) {
        Collections.sort(transactionList);
        ck.insertOne(new Document()
                .append("_id", id)
                .append("count", count)
                .append("items", getCkDoc(lkDoc))
                .append("transactions", transactionList));
        id++;
    }

    private static Document getCkDoc(List<Integer> lkDoc) {
        Document document = new Document();
        for (int i = 0; i < lkDoc.size(); i++) {
            document.append("pos_" + i, lkDoc.get(i));
        }
        return document;
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
