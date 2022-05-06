package edu.rit.ibd.a6;

import com.google.common.collect.Sets;
import com.mongodb.client.FindIterable;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.*;

public class AprioriGen {

    public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColLKMinusOne = args[2];
		final String mongoColCK = args[3];

        /*final String mongoDBURL = "None";
        final String mongoDBName = "Ass6_2";
        final String mongoColLKMinusOne = "L4_5_3";
        final String mongoColCK = "C2_1_3";*/

        MongoClient client = getClient(mongoDBURL);
        MongoDatabase db = client.getDatabase(mongoDBName);

        MongoCollection<Document> lKMinusOne = db.getCollection(mongoColLKMinusOne);
        MongoCollection<Document> ck = db.getCollection(mongoColCK);

        // TODO Your code here!

        /*
         *
         * First, you must figure out the current k-1 by checking the number of items in the input collection.
         *
         * Then, you must start two pointers p and q such that p.pos_0==q.pos_0 AND p.pos_1==q.pos_1 AND ... AND p.pos_k-2==q.pos_k-2. Furthermore,
         * 	p.pos_k-1<q.pos_k-1.
         *
         * If the previous condition is true, a new document d as follows is candidate to be added:
         * 		p.pos_0, p.pos_1, ... p.pos_k-2, p.pos_k-1, q.pos_k-1
         *
         * Before adding it, we must check that all its subsets of size (k-1) were present in Lk-1. Use Sets.combinations to get each of these subsets.
         * 	If for a given subset s, there is no document that contains s, the previous document d is pruned.
         *
         * Otherwise, d is added to Ck.
         *
         */

        // You must figure out the value of k - 1.

        // You can implement this "by hand" using Java, an aggregation query, or a mix.

        // Remember that there is the join and the prune steps. Use Sets.combinations for the prune step.
        // Skip the prune step for L1.
        int kMinusOne = 0;
        Document document = lKMinusOne.find().first();
        if (document != null)
            kMinusOne = ((Document) document.get("items")).size();
        FindIterable<Document> pIter = lKMinusOne.find().batchSize(100);
        FindIterable<Document> qIter = lKMinusOne.find().batchSize(100);
        ArrayList<Document> lkMinusOneList = new ArrayList<>();
        lKMinusOne.find().batchSize(100).into(lkMinusOneList);
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
                        if (kMinusOne > 1) {
                            if (!prune(lkDoc, lkMinusOneList, kMinusOne)) {
                                addToCk(lkDoc, ck);
                            }
                        } else {
                            addToCk(lkDoc, ck);
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

    private static void addToCk(List<Integer> lkDoc, MongoCollection<Document> ck) {
        ck.insertOne(new Document()
                .append("count", 0)
                .append("items", getCkDoc(lkDoc)));
    }

    private static Document getCkDoc(List<Integer> lkDoc) {
        Document document = new Document();
        for (int i = 0; i < lkDoc.size(); i++) {
            document.append("pos_" + i, lkDoc.get(i));
        }
        return document;
    }

    private static boolean prune(List<Integer> lkDoc, ArrayList<Document> lkMinusOneList, int kMinusOne) {
        boolean prune = false;
        for (Set<Integer> combination : Sets.combinations(new LinkedHashSet<>(lkDoc), kMinusOne)) {
            if (!checkIfCombinationExists(combination, lkMinusOneList)) {
                prune = true;
            }
        }
        return prune;
    }

    private static boolean checkIfCombinationExists(Set<Integer> combination, ArrayList<Document> lKMinusOne) {
        for (Document document : lKMinusOne) {
            List<Integer> items = getList((Document) document.get("items"));
            if (items.containsAll(combination)){
                return true;
            }
        }
        return false;
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
