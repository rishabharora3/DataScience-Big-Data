package edu.rit.ibd.a7;


import ch.obermuhlner.math.big.BigDecimalMath;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Arrays;

public class MutualInformation {

    public static void main(String[] args) throws Exception {
        final String mongoDBURL = args[0];
        final String mongoDBName = args[1];
        final String mongoCol = args[2];
        final int R = Integer.valueOf(args[3]);
        final int C = Integer.valueOf(args[4]);

        /*final String mongoDBURL = "None";
        final String mongoDBName = "Kmeans";
        final String mongoCol = "Points_0_5";
        final int R = Integer.valueOf("83");
        final int C = Integer.valueOf("50");*/

        MongoClient client = getClient(mongoDBURL);
        MongoDatabase db = client.getDatabase(mongoDBName);

        MongoCollection<Document> collection = db.getCollection(mongoCol);
//        Document doc = collection.find().first();
//        System.out.println(doc);


        // TODO Your code here!

        /*
         *
         * Every point will have two labels: label_u is an integer indicating the cluster of the point in assignment U; similarly, label_v is the
         * 	cluster (integer) of the point in assignment V. |U|=R and |V|=C.
         *
         * You need to compute fields a, b and c. Field a is an array of size R in which each position a.i indicates the total number of points
         * 	assigned to cluster i in U. Similarly, b is an array of size C; each position b.j is the total number of points assigned to cluster j
         * 	in V. Field c is an array storing the contingency matrix. In the contingency matrix, [i, j] is the number of points that are assigned
         * 	to both i in U and j in V. Since MongoDB does not allow us to store matrices, we are going to store the matrix as a single array.
         * 	Note that the contingency matrix has R rows and C columns. Cell [i, j] is position p=i*C+j in the array c. Furthermore, position p in
         * 	c corresponds to the following cell: [(p - p%C)/C, p%C].
         *
         * Using the previous fields, you need to compute H(U), H(V), MI and E(MI) as defined in the slides. You must store them in the following
         * 	fields: hu, hv, mi and emi, respectively.
         *
         * To compute the factorial part of E(MI), you should use log factorial. The following library is a good resource to work with BigDecimal
         * 	in Java: https://github.com/eobermuhlner/big-math
         *
         * All these fields must be stored in a single document with _id=mi_info.
         *
         */
        BigDecimal[] a = new BigDecimal[R];
        BigDecimal[] b = new BigDecimal[C];
        BigDecimal[] c = new BigDecimal[R * C];
        Arrays.fill(a, BigDecimal.ZERO);
        Arrays.fill(b, BigDecimal.ZERO);
        Arrays.fill(c, BigDecimal.ZERO);
        BigDecimal total = BigDecimal.ZERO;
        total = updateABC(C, collection, a, b, c, total);
        BigDecimal hu = computeH(a, total);
        BigDecimal hv = computeH(b, total);
        BigDecimal mi = computeMI(a, b, c, total, R, C);
        collection.insertOne(new Document("_id", "mi_info")
                .append("a", Arrays.asList(a))
                .append("b", Arrays.asList(b))
                .append("c", Arrays.asList(c))
                .append("n", total)
                .append("hu", hu)
                .append("hv", hv)
                .append("mi", mi).append("emi", computeEMI(mi, hu, hv)));
        // TODO End of your code!
        client.close();
    }

    private static BigDecimal computeEMI(BigDecimal mi, BigDecimal hu, BigDecimal hv) {
        return new BigDecimal(0);
    }

    private static BigDecimal computeMI(BigDecimal[] a, BigDecimal[] b, BigDecimal[] c, BigDecimal total, int R, int C) {
        BigDecimal mi = BigDecimal.ZERO;
        for (int i = 0; i < R; i++) {
            for (int j = 0; j < C; j++) {
                if (c[i * C + j].compareTo(BigDecimal.ZERO) == 0)
                    continue;
                Double temp2 = Math.log(c[i * C + j].multiply(total.pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128).doubleValue()) - (Math.log(a[i].multiply(total.pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128).doubleValue())
                        + Math.log(b[j].multiply(total.pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128).doubleValue()));
                mi = mi.add(c[i * C + j].multiply(total.pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128).multiply(new BigDecimal(temp2, MathContext.DECIMAL128), MathContext.DECIMAL128), MathContext.DECIMAL128);
            }
        }
        return mi;
    }

    private static BigDecimal computeH(BigDecimal[] a, BigDecimal total) {
        BigDecimal hu = BigDecimal.ZERO;
        for (BigDecimal ai : a) {
            if (ai.compareTo(BigDecimal.ZERO) == 0)
                continue;
            BigDecimal temp = ai.multiply(total.pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128);
            hu = hu.subtract(temp.multiply(BigDecimalMath.log(temp, MathContext.DECIMAL128), MathContext.DECIMAL128), MathContext.DECIMAL128);
        }
        return hu;
    }

    private static BigDecimal updateABC(int C, MongoCollection<Document> collection, BigDecimal[] a, BigDecimal[] b, BigDecimal[] c, BigDecimal total) {
        for (Document document : collection.find(Document.parse("{_id: /^p_/}")).batchSize(100)) {
            if (document.getInteger("label_u") != null && document.getInteger("label_v") != null) {
                total = total.add(BigDecimal.ONE, MathContext.DECIMAL128);
                a[document.getInteger("label_u")] = a[document.getInteger("label_u")].add(BigDecimal.ONE, MathContext.DECIMAL128);
                b[document.getInteger("label_v")] = b[document.getInteger("label_v")].add(BigDecimal.ONE, MathContext.DECIMAL128);
                c[document.getInteger("label_u") * C + document.getInteger("label_v")] = c[document.getInteger("label_u") * C + document.getInteger("label_v")].add(BigDecimal.ONE, MathContext.DECIMAL128);
            }
        }
        return total;
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
