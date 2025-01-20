package Document_and_Data;

import java.io.Serializable;
import java.util.List;

public class SearchParameters implements Serializable {
    private String searchQuery;
    private List<Document> documents;

    public SearchParameters(String searchQuery, List<Document> documents) {
        this.searchQuery = searchQuery;
        this.documents = documents;
    }

    public String getSearchQuery() {
        return searchQuery;
    }

    public List<Document> getDocuments() {
        return documents;
    }
    public void setSearchQuery(String searchQuery) {
        this.searchQuery = searchQuery;
    }
    public void setDocuments(List<Document> documents) {
        this.documents = documents;
    }
}

