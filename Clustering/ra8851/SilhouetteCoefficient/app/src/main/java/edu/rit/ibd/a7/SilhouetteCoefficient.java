package edu.rit.ibd.a7;

import ch.obermuhlner.math.big.BigDecimalMath;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class SilhouetteCoefficient {
    public enum Distance {Manhattan, Euclidean}


    public enum Mean {Arithmetic, Geometric}


    public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoCol = args[2];
		final String pointId = args[3];
		final Distance distance = Distance.valueOf(args[4]);
		final Mean mean = Mean.valueOf(args[5]);


//        final String mongoDBURL = "None";
//        final String mongoDBName = "Kmeans";
//        final String mongoCol = "Points_0_5";
//        final String pointId = "p_1155696370677";
//        final Distance distance = Distance.valueOf("Manhattan");
//        final Mean mean = Mean.valueOf("Arithmetic");

        MongoClient client = getClient(mongoDBURL);
        MongoDatabase db = client.getDatabase(mongoDBName);

        MongoCollection<Document> collection = db.getCollection(mongoCol);

        // TODO Your code here!

        /*
         *
         * You need to compute a and d_i only for the input point (pointId). Let's assume it is p_x
         *
         * a is the mean (arithmetic or geometric) of the distances of p_x to all other points assigned to the same centroid as p_x, excluding p_x.
         *
         * d_i is the mean (arithmetic or geometric) of the distances of p_x to all other points assigned to centroid c_i.
         *
         * Note that, if p_x is assigned to c_j, d_j should not exist.
         *
         */
        LinkedHashMap<String, BigDecimal> distances = computeDistance(collection, pointId, distance, mean);
        pushDistances(collection, pointId, distances);


        // TODO End of your code!

        client.close();
    }

    private static void pushDistances(MongoCollection<Document> collection, String pointId, HashMap<String, BigDecimal> distances) {
        Document point = collection.find(new Document("_id", pointId)).first();
        if (point == null)
            return;
        point.append("a", distances.get(point.getString("label")));
        distances.remove(point.getString("label"));
        for (String key : distances.keySet()) {
            point.append("d_" + key.replace("c_", ""), distances.get(key));
            collection.updateOne(new Document("_id", pointId), new Document("$set", point));
        }
    }

    private static LinkedHashMap<String, BigDecimal> computeDistance(MongoCollection<Document> collection, String pointId, Distance distance, Mean mean) {
        Document point = collection.find(new Document("_id", pointId)).first();
        LinkedHashMap<String, BigDecimal> distances = new LinkedHashMap<>();
        for (Document document : collection.find(Document.parse("{_id: /^c_/}")).batchSize(100)) {
            String centroidId = document.get("_id", String.class);
            BigDecimal total = BigDecimal.ZERO;
            for (Document pointDocument : collection.find(new Document("label", centroidId)).batchSize(100)) {
                if (!pointDocument.get("_id", String.class).equals(pointId)) {
                    total = total.add(BigDecimal.ONE, MathContext.DECIMAL128);
                    switch (mean) {
                        case Arithmetic -> distances.put(centroidId, distances.getOrDefault(centroidId, BigDecimal.ZERO).add(getDistance(pointDocument, point, distance), MathContext.DECIMAL128));
                        case Geometric -> distances.put(centroidId, distances.getOrDefault(centroidId, BigDecimal.ZERO)
                                .add(BigDecimalMath.log(getDistance(pointDocument, point, distance), MathContext.DECIMAL128), MathContext.DECIMAL128));
                    }
                }
            }
            if (total.compareTo(BigDecimal.ZERO) != 0) {
                switch (mean) {
                    case Arithmetic -> distances.put(centroidId, distances.get(centroidId).multiply(total.pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128));
                    case Geometric -> distances.put(centroidId, BigDecimalMath.exp(distances.get(centroidId)
                            .multiply(total.pow(-1, MathContext.DECIMAL128), MathContext.DECIMAL128), MathContext.DECIMAL128));
                }
            }
        }
        return distances;
    }

    private static BigDecimal getDistance(Document centroid, Document document, Distance distance) {
        return switch (distance) {
            case Euclidean -> getEuclideanDistance(centroid, document);
            case Manhattan -> getManhattanDistance(centroid, document);
        };
    }

    private static BigDecimal getManhattanDistance(Document centroid, Document document) {
        BigDecimal distance = BigDecimal.ZERO;
        Document pointOne = centroid.get("point", Document.class);
        Document pointTwo = document.get("point", Document.class);
        for (int i = 0; i < pointOne.size(); i++) {
            BigDecimal dimCentroid = pointOne.get("dim_" + i, Decimal128.class).bigDecimalValue();
            BigDecimal dim = pointTwo.get("dim_" + i, Decimal128.class).bigDecimalValue();
            distance = distance.add(dimCentroid.subtract(dim, MathContext.DECIMAL128).abs(), MathContext.DECIMAL128);
        }
        return distance;
    }

    private static BigDecimal getEuclideanDistance(Document centroid, Document document) {
        BigDecimal distance = BigDecimal.ZERO;
        Document pointOne = centroid.get("point", Document.class);
        Document pointTwo = document.get("point", Document.class);
        for (int i = 0; i < pointOne.size(); i++) {
            BigDecimal dimCentroid = pointOne.get("dim_" + i, Decimal128.class).bigDecimalValue();
            BigDecimal dim = pointTwo.get("dim_" + i, Decimal128.class).bigDecimalValue();
            distance = distance.add(dimCentroid.subtract(dim, MathContext.DECIMAL128).pow(2, MathContext.DECIMAL128), MathContext.DECIMAL128);
        }
        return distance.sqrt(MathContext.DECIMAL128);
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
