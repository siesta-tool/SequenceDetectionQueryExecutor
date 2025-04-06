package com.datalab.siesta.queryprocessor.services;

import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.storage.DBConnector;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This class is running during initialization and loads information from the storage. Specifically, it loads the metadata
 * for all the log databases, and also for each log database loads the different event types.
 */
@Service
@ComponentScan
@Getter
public class LoadInfo {

    private DBConnector dbConnector;


    private Map<String, Metadata> metadata;
    private Map<String, List<String>> eventTypes;
    private Map<String, Map<String, Long>> eventTypeOccurrences;


    @Autowired
    public LoadInfo(DBConnector dbConnector) {
        this.dbConnector = dbConnector;
        reloadAll(); // initial load
    }

    public void reloadAll() {
        this.metadata = loadMetadata();
        this.eventTypes = loadEventTypes();
        this.eventTypeOccurrences = loadEventTypeOccurrences();
    }

    private Map<String, Metadata> loadMetadata() {
        Map<String, Metadata> m = new HashMap<>();
        for (String l : dbConnector.findAllLongNames()) {
            Metadata metadata = dbConnector.getMetadata(l);
            if(metadata == null) { //might be an incomplete logdatabase
                continue;
            }
            // TODO: determine a way to find the starting ts
            if(metadata.getStart_ts()==null){
                metadata.setStart_ts("");
            }
            m.put(l, metadata);
        }
        return m;
    }

    private Map<String, List<String>> loadEventTypes() {
        Map<String, List<String>> response = new HashMap<>();
        for (String l : dbConnector.findAllLongNames()) {
            response.put(l, dbConnector.getEventNames(l));
        }
        return response;
    }

    private Map<String, Map<String,Long>> loadEventTypeOccurrences() {
        Map<String, Map<String,Long>> response = new HashMap<>();
        for (String l : dbConnector.findAllLongNames()) {
            response.put(l, dbConnector.getEventTypeOccurrences(l));
        }
        return response;
    }
}
