import Document_and_Data.Document;
import Document_and_Data.DocumentTermsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/leader")
public class Leader {

    private static final List<DocumentTermsInfo> documentTermsInfo = Collections.synchronizedList(new ArrayList<>());
    private static final String FOLDER_PATH = "D:\\4 and 5\\five\\Ds\\project\\ds_project_part1\\My-TF-IDF\\src\\main\\resources\\documents"; // Update as needed
    private static final Logger logger = LoggerFactory.getLogger(Leader.class);

    private static final String WORKER_URL = "http://localhost:8081/worker/process"; // Update worker's URL if needed

    @PostMapping("/start")
    public TreeMap<String, Double> start(@RequestBody String searchQuery) {
        RestTemplate restTemplate = new RestTemplate();

        // Send search parameters to the worker and get the results
        List<DocumentTermsInfo> workerResponse = restTemplate.postForObject(WORKER_URL, searchQuery, List.class);

        if (workerResponse == null || workerResponse.isEmpty()) {
            logger.warn("No results returned from the worker.");
            return new TreeMap<>();
        }
        List<Document> ducuments = getDocumentsFromResources();
        // Calculate IDF and document scores
        Map<String, Double> idfs = calculateIDF(workerResponse, ducuments.size(),
                searchQuery);
        Map<Document, Double> documentScores = calculateDocumentsScore(idfs, workerResponse);

        // Sort and return the results
        return sortDocumentsScoresByName(documentScores);
    }

    private List<Document> getDocumentsFromResources() {
        try {
            return Files.walk(Paths.get(FOLDER_PATH))
                    .filter(Files::isRegularFile)
                    .map(path -> new Document(path.getFileName().toString()))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.error("Error reading documents: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private Map<String, Double> calculateIDF(List<DocumentTermsInfo> documentTermsInfo, double totalDocuments, String searchQuery) {
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

    private Map<Document, Double> calculateDocumentsScore(Map<String, Double> idfs, List<DocumentTermsInfo> documentTermsInfo) {
        Map<Document, Double> documentScores = new HashMap<>();
        for (DocumentTermsInfo termsInfo : documentTermsInfo) {
            double score = termsInfo.getTermFrequency().entrySet().stream()
                    .mapToDouble(entry -> idfs.getOrDefault(entry.getKey(), 0.0) * entry.getValue())
                    .sum();
            documentScores.put(termsInfo.getDocument(), score);
        }
        return documentScores;
    }

    private TreeMap<String, Double> sortDocumentsScoresByName(Map<Document, Double> documentScores) {
        return documentScores.entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().getName(), Map.Entry::getValue, (e1, e2) -> e1, TreeMap::new));
    }
}
