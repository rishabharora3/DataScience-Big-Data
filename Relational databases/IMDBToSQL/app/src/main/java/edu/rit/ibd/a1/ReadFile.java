package edu.rit.ibd.a1;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

public class ReadFile {
    public static void main(String[] args) throws IOException {
        String[] array = new String[]{"name.basics.tsv.gz", "title.basics.tsv.gz", "title.crew.tsv.gz",
                "title.principals.tsv.gz", "title.ratings.tsv.gz"};
        final String folderToIMDBGZipFiles = "/Users/rishabharora/Downloads/RIT SEMS/Sem2RIT/CSCI 620 Big Data/Assignments/Assignment 1/gzfiles/";
        for (String fileName : array) {
            System.out.println("\n\n\nPRINTING FILE : " + fileName + "\n\n\n\n");
            InputStream gzipStream = new GZIPInputStream(
                    new FileInputStream(folderToIMDBGZipFiles + fileName));
            Scanner sc = new Scanner(gzipStream, StandardCharsets.UTF_8);
            int count = 0;
            while (sc.hasNextLine() && count <= 1000) {
                String line = sc.nextLine();
                line = line.replaceAll("\\t", "    |    ");
                System.out.println(line);
                System.out.println("-----------------------------------------------------------------------------------");
                count++;
            }
            sc.close();
        }
    }
}
