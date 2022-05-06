package edu.rit.ibd.a5;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

public class DeRefMongo {

    public static void main(String[] args) throws Exception {
        final String mongoDBURL = args[0];
        final String mongoDBName = args[1];
        final String jsonFile = args[2];

        MongoClient client = getClient(mongoDBURL);
        MongoDatabase db = client.getDatabase(mongoDBName);
        MongoCollection<Document> movieCollection = db.getCollection("Movies");


        // TODO 0: Your code here!
        /*
         * Read the MovieInfo file that contains a number of JSON documents. For each document, find a document in the
         * 	Movies collection that matches using de-referencing. Update the document in MongoDB by including the expected
         * 	field(s) with appropriate type.
         *
         * Note that the ids of the documents in the MovieInfo file are the IMDB ids, e.g., ttXYZ, so you must use XYZ.
         *
         * A document d in the Movies collection has d.id-conflicts++ if there exists another document x in the MovieInfo
         * 	file such that d._id == x.id and d.bechdel-test-id != x.resultLabel.
         *
         * A document d in the Movies collection has d.title-conflicts++ if there exists another document x in the MovieInfo
         * 	file such that d.otitle = x.title and d._id != x.id (regardless of the x.resultLabel value).
         */
        List<Document> movieInfoDocs;
        movieCollection.createIndex(new Document("otitle", 1));
//        movieCollection.createIndex(new Document("_id", 1));
        BufferedReader br = new BufferedReader(new FileReader(jsonFile));
        Document data = Document.parse("{\"data\":" + br.readLine() + "}");
        br.close();
        movieInfoDocs = (List<Document>) data.get("data");
        for (Document document : movieInfoDocs) {
            String id = document.getString("id");
            String resultLabel = document.getString("resultLabel");
            if (!id.contains("tt") || resultLabel == null)
                continue;
            int docId = Integer.parseInt(id.replace("tt", ""));
            String title = document.getString("title");
            Document docDefrById = dereferenceId(movieCollection, id, docId, title, resultLabel);
            dereferenceTitle(movieCollection, id, docId, title, resultLabel, docDefrById);
        }

        // Read the file and load the docs in the list. Alternatively, you can also load them in a new collection in MongoDB.

        // resultLabel is the result of the Bechdel test (passes/fails) for a given movie. When you are dereferencing using
        //	id, you should set the bechdel-test-id field in the database. When you are dereferencing using title, you should
        //	set the bechdel-test-title field in the database.

        // When dereferencing using id:
        //	Let dInDB be a document in the database, and dInFile be a document in the file such that dInDB._id == dInFile._id.
        //	If dInDB.bechdel-test-id != dInFile.resultLabel, then dInDB.id-conflicts++.

        // When dereferencing using title:
        //	Let dInDB be a document in the database, and dInFile be a document in the file such that dInDB.otitle == dInFile.title.
        //	If dInDB._id != dInFile._id, then dInDB.title-conflicts++. Let otherDInDB be another document in the database such that
        //		otherDInDB._id=dInFile._id, then otherDInDB.title-conflicts++.

        // TODO 0: End of your code.
        movieCollection.dropIndex(new Document("otitle", 1));
//        movieCollection.dropIndex(new Document("_id", 1));
        client.close();
    }

    private static void dereferenceTitle(MongoCollection<Document> movieCollection, String id, int
            docId, String title, String resultLabel, Document document1) {
        FindIterable<Document> iterDoc = movieCollection.find(Filters.eq("otitle", title));
        for (Document document : iterDoc) {
            String bechdelTestTitle = document.getString("bechdel-test-title");
            Document updateDocument = new Document();
            if (bechdelTestTitle == null && !resultLabel.isEmpty()) {
                updateDocument.append("bechdel-test-title", resultLabel);
                movieCollection.updateOne(Filters.eq("_id", document.getInteger("_id")),
                        new Document().append("$set", updateDocument));
                System.out.println("bechdel-test-title inserted");
                System.out.println(id);
                System.out.println(title);
                System.out.println(resultLabel);
            } else if (document.getInteger("_id") != docId && (document1 != null)) {
                int titleConflicts = updateConflict(movieCollection, document.getInteger("_id"), document, updateDocument, "title-conflicts");
                updateConflict(movieCollection, docId, document1, new Document(), "title-conflicts");
                System.out.println("Conflict updated count - " + (titleConflicts == -1 ? 1 : titleConflicts));
                System.out.println(id);
                System.out.println(title);
                System.out.println(resultLabel);
            }
        }
    }

    private static Document dereferenceId(MongoCollection<Document> movieCollection, String id, int docId, String title, String resultLabel) {
        FindIterable<Document> iterDoc = movieCollection.find(Filters.eq("_id", docId));
        for (Document document : iterDoc) {
            String bechdelTestId = document.getString("bechdel-test-id");
            Document updateDocument = new Document();
            if (bechdelTestId == null && !resultLabel.isEmpty()) {
                updateDocument.append("bechdel-test-id", resultLabel);
                movieCollection.updateOne(Filters.eq("_id", docId),
                        new Document().append("$set", updateDocument));
                System.out.println("bechdel-test-id inserted");
                System.out.println(id);
                System.out.println(title);
                System.out.println(resultLabel);
            } else if (!resultLabel.isEmpty() && !resultLabel.equals(bechdelTestId)) {
                int idConflicts = updateConflict(movieCollection, docId, document, updateDocument, "id-conflicts");
                System.out.println("Conflict updated count - " + (idConflicts == -1 ? 1 : idConflicts));
                System.out.println(id);
                System.out.println(title);
                System.out.println(resultLabel);
            }
            return document;
        }
        return null;
    }

    private static int updateConflict(MongoCollection<Document> movieCollection, int docId, Document document,
                                      Document updateDocument, String s) {
        int idConflicts = document.getInteger(s, -1);
        if (idConflicts == -1)
            updateDocument.append(s, 1);
        else
            updateDocument.append(s, ++idConflicts);
        movieCollection.updateOne(Filters.eq("_id", docId),
                new Document().append("$set", updateDocument));
        return idConflicts;
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
