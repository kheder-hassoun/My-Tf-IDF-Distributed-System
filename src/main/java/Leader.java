import Document_and_Data.Document;
import Document_and_Data.DocumentTermsInfo;
import Document_and_Data.SearchParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.Doc;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class Leader {

    // List to store document terms information
    private static List<DocumentTermsInfo> documentTermsInfo = Collections.synchronizedList(new ArrayList<>());
    private static final String FOLDER_PATH = "D:\\4 and 5\\five\\Ds\\project\\ds_project_part1\\registration-discovery-leader-reelection-cordinator-workers\\src\\main\\resources\\documents";
    private static final Logger logger = LoggerFactory.getLogger(Leader.class);

    // Main method to initiate the search process
    public static TreeMap<String, Double> start(String searchQuery) throws IOException, InterruptedException {
        logger.info("Search Query: {}", searchQuery);

        // Validate search query
        if (searchQuery == null || searchQuery.isEmpty()) {
            logger.warn("Please enter a search query!");
            return null;
        }

        // Load documents from resources
        List<Document> documents = getDocumentsFromResources();
        if (documents.isEmpty()) {
            logger.warn("No documents found!");
            return null;
        }

        // Get all available service addresses
        List<String> serviceAddresses = ServiceRegistry.getAllServiceAddresses();
        if (serviceAddresses.isEmpty()) {
            logger.warn("No service addresses available. Try again later!");
            return null;
        }

        // Distribute documents among workers
        distributeDocumentsToWorkers(serviceAddresses, documents, searchQuery);

        // Calculate IDF and document scores after processing
        Map<String, Double> idfs = calculateIDF(documentTermsInfo, (double) documents.size(), searchQuery);
        Map<Document, Double> documentScores = calculateDocumentsScore(idfs, documentTermsInfo);

        // Sort and display the results
        TreeMap<String, Double> sortedScores = sortDocumentsScoresByName(documentScores);
        displaySortedScoresByName(sortedScores);

        return sortedScores;
    }

    // Distribute documents across available workers and start the search
    private static void distributeDocumentsToWorkers(List<String> addresses, List<Document> documents, String searchQuery) throws InterruptedException {
        System.out.println("Distribute documents across available workers and start the search");
        System.out.println("doucumemt list : "+ documents);
        for(Document doc : documents){
            System.out.println(doc.getName());
        }
        System.out.println(" doucument size : "+documents.size());
        int documentsPerWorker = documents.size() / addresses.size();
        int remainingDocuments = documents.size() % addresses.size();
        int startIndex = 0;

        List<Thread> threads = new ArrayList<>();
        for (String address : addresses) {
            int endIndex = startIndex + documentsPerWorker + (remainingDocuments-- > 0 ? 1 : 0);
            List<Document> subDocuments = documents.subList(startIndex, endIndex);
            startIndex = endIndex;

            SearchParameters searchParameters = new SearchParameters(searchQuery, subDocuments);
            Thread thread = new Thread(() -> {
                try {
                    startSearchOnWorker(address, searchParameters);
                } catch (IOException e) {
                    logger.error("Error searching on worker {}: {}", address, e.getMessage());
                }
            });
            threads.add(thread);
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }
    }

    // Perform search on a worker using the given address and search parameters
    private static void startSearchOnWorker(String workerAddress, SearchParameters searchParameters) throws IOException {
        System.out.println("Perform search on a worker using the given address and search parameters");
        System.out.println("my doucment size : "+searchParameters.getDocuments().size());
        System.out.println("the serach query :"+searchParameters.getSearchQuery());
        String[] addressParts = workerAddress.split(":");
        System.out.println(" ************ 1");
        String ipAddress = addressParts[0];
        System.out.println("ip  "+ ipAddress);
        System.out.println(" ************ 2");

        int port = Integer.parseInt(addressParts[1]);
        System.out.println(" ************ 3");
        System.out.println("port "+ port);



        try (Socket socket = new Socket(ipAddress, port);

             ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream())
        ) {

            // Send search parameters to the worker
            System.out.println("send to worker");
            outputStream.writeObject(searchParameters);

            // Receive and store the results
            List<DocumentTermsInfo> results = (List<DocumentTermsInfo>) inputStream.readObject();
            synchronized (documentTermsInfo) {
                documentTermsInfo.addAll(results);
            }
        } catch (Exception e) {
            logger.error("Error communicating with worker {}: {}", workerAddress, e.getMessage());
        }
        System.out.println("test catch ");
    }

    // Calculate IDF for each term in the search query
    private static Map<String, Double> calculateIDF(List<DocumentTermsInfo> documentTermsInfo, double totalDocuments, String searchQuery) {
        Map<String, Double> wordDocumentCount = new HashMap<>();
        for (DocumentTermsInfo termsInfo : documentTermsInfo) {
            termsInfo.getTermFrequency().keySet().forEach(term -> wordDocumentCount.merge(term, 1.0, Double::sum));
        }

        Map<String, Double> idfs = new HashMap<>();
        for (String term : searchQuery.split("\\s+")) {
            double documentCount = wordDocumentCount.getOrDefault(term, 0.0);
            idfs.put(term, documentCount > 0 ? Math.log10(totalDocuments / documentCount) : 0.0);
        }
        return idfs;
    }

    // Calculate scores for all documents based on IDF and term frequency
    private static Map<Document, Double> calculateDocumentsScore(Map<String, Double> idfs, List<DocumentTermsInfo> documentTermsInfo) {
        Map<Document, Double> documentScores = new HashMap<>();
        for (DocumentTermsInfo termsInfo : documentTermsInfo) {
            double score = termsInfo.getTermFrequency().entrySet().stream()
                    .mapToDouble(entry -> idfs.getOrDefault(entry.getKey(), 0.0) * entry.getValue())
                    .sum();
            documentScores.put(termsInfo.getDocument(), score);
        }
        return documentScores;
    }

    // Load documents from the specified folder
    private static List<Document> getDocumentsFromResources() {
        try {
            return Files.walk(Paths.get(FOLDER_PATH))
                    .filter(Files::isRegularFile)
                    .map(path -> new Document(path.getFileName().toString()))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            logger.error("Error reading documents: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    // Sort document scores by document name
    private static TreeMap<String, Double> sortDocumentsScoresByName(Map<Document, Double> documentScores) {
        return documentScores.entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().getName(), Map.Entry::getValue, (e1, e2) -> e1, TreeMap::new));
    }

    // Display sorted document scores by name
    private static void displaySortedScoresByName(TreeMap<String, Double> sortedScores) {
        logger.info("Sorted Document Scores:");
        sortedScores.forEach((doc, score) -> logger.info("Document: {}, Score: {}", doc, score));
    }
}
