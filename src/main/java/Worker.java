import Document_and_Data.Document;
import Document_and_Data.DocumentTermsInfo;
import Document_and_Data.SearchParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Worker {
    private int PORT = 12345;
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    public Worker(int port) {
        this.PORT = port;
    }

    public void start() {
        System.out.println(" ************ worker stated");

        // Start the worker to receive the list of documents
        receiveDataForSearchFromLeader();
    }

    private void receiveDataForSearchFromLeader() {



        // Create a server socket to listen for incoming connections
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println(" ************ 1");

            // Infinite loop to continuously listen for connections
            while (true) {
                // Accept incoming connection from the leader
                Socket socket = serverSocket.accept();
                System.out.println(" ************ 2");

                // Create an ObjectInputStream for receiving the list of documents
                ObjectInputStream objectStream = new ObjectInputStream(socket.getInputStream());
                System.out.println(" ************ 3");

                // Receive the list of documents from the leader

                SearchParameters receivedData = (SearchParameters) objectStream.readObject();
                System.out.println(" ************ 4");
                System.out.println(receivedData);

                logger.info("Search query: {}", receivedData.getSearchQuery());
                // Process the received list of documents
                for (Document document : receivedData.getDocuments()) {
                    logger.info("Received document: {}", document.getName());
                }
                logger.info("");

                List<DocumentTermsInfo> documentTermsInfos = calculateDocumentScores(receivedData.getDocuments(), receivedData.getSearchQuery());

                sendDocumentTermsInfoToLeader(socket, documentTermsInfos);
                // Close the socket connection
                socket.close();
            }
        } catch (IOException | ClassNotFoundException e) {
            // Handle connection or communication error
            e.printStackTrace();
            logger.error("An error occurred while receiving data from leader", e);
        }
        ///******************
    }
    private List<DocumentTermsInfo> calculateDocumentScores(List<Document> documents, String searchQuery) {

        List<DocumentTermsInfo> documentTermsInfos = new ArrayList<>();

        // Split the search query into words
        String[] queryWords = searchQuery.split("\\s+");

        // Calculate the score for each document
        for (Document document : documents) {
            String filePath = "D:\\4 and 5\\five\\Ds\\project\\ds_project_part1\\registration-discovery-leader" +
                    "-reelection-cordinator-workers\\src\\main\\resources\\documents\\" + document.getName();
            // Complete path to your file

            File file = new File(filePath);

            if (!file.exists()) {
                logger.warn("File not found: {}", filePath);
                continue;
            }

            // Open an input stream to read the file
            try (InputStream inputStream = new FileInputStream(file)) {
                // Read the file contents
                byte[] data = inputStream.readAllBytes();
                String fileContent = new String(data);

                // Calculate the score for the document based on the search query
                DocumentTermsInfo documentTermsInfo = calculateScore(fileContent, queryWords,document.getName());

                documentTermsInfos.add(documentTermsInfo);
            } catch (IOException e) {
                logger.error("An error occurred while calculating the document scores", e);
                e.printStackTrace();
            }
        }
        return documentTermsInfos;
    }
    private DocumentTermsInfo calculateScore(String fileContent, String[] queryWords, String docName) {
        DocumentTermsInfo documentTermsInfo = new DocumentTermsInfo();
        Document document = new Document(docName);
        documentTermsInfo.setDocument(document);
        HashMap<String,Double> termsInfo = new HashMap<String,Double>();
        double TF = 0.0;
        logger.info("Results in {} Document:", docName);
        for (String word : queryWords) {
            Double termFrequency = 0.0;
            double wordCountInDocument = countWordOccurrences(fileContent, word);
            String[] documentWords = fileContent.split("\\s+");
            termFrequency = calculateTermFrequency(wordCountInDocument,documentWords.length);
            logger.info("(Term: {}, Frequency: {}, Total Document Words: {}, Term Frequency (Percentage): {})", word, wordCountInDocument, documentWords.length, termFrequency);
            termsInfo.put(word,termFrequency);
        }
        logger.info("................................................");
        documentTermsInfo.setTermFrequency(termsInfo);
        return documentTermsInfo;
    }

    private double countWordOccurrences(String fileContent, String word) {
        String[] documentWords = fileContent.split("\\s+");
        double count = 0;
        for (String documentWord : documentWords) {
            if (documentWord.toLowerCase().contains(word.toLowerCase())) {
                count++;
            }
        }
        return count;
    }
    private double calculateTermFrequency(double wordCountInDocument,double documentWordsCount) {
        double TF = 0.0;
        if(documentWordsCount != 0){
            TF = (wordCountInDocument / documentWordsCount);
        }
        return TF;
    }
    private void sendDocumentTermsInfoToLeader(Socket socket, List<DocumentTermsInfo> documentTermsInfo) {
        try {
            // Create an ObjectOutputStream for sending the document scores
            ObjectOutputStream objectStream = new ObjectOutputStream(socket.getOutputStream());
            // Send the document scores to the leader
            objectStream.writeObject(documentTermsInfo);
            // Flush and close the object stream
            objectStream.flush();
            objectStream.close();
        } catch (IOException e) {
            // Handle communication error
            e.printStackTrace();
            logger.error("An error occurred while sending results to leader", e);
        }
    }
}

