package com.example.AtyponDatabase.Services;

import com.example.AtyponDatabase.Database.DocumentIndex;
import com.example.AtyponDatabase.Managers.DatabaseManager;
import com.example.AtyponDatabase.Validation.DocumentValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.util.*;

import java.io.File;
import java.io.IOException;

@Service
public class DocumentService {
    @Autowired
    private DocumentValidator documentValidator;
    public String createDocument(String databaseName, String collectionName, String fileName, String schema) throws IOException {
        DatabaseManager.getInstance().getDatabases().get(databaseName).getCollections().get(collectionName).getDocumentLock().writeLock().lock();
        try {
            String collectionFolderPath = System.getProperty("user.dir") + "/databases/" + databaseName + "/" + collectionName;
            if (fileName.equals("false")) {
                fileName = UUID.randomUUID().toString();
            }

            Map<String, String> docMap = readDocument(schema, fileName);
            if (isValid(collectionFolderPath, docMap)) {
                createDocumentFile(databaseName, collectionName, docMap, fileName);
                createIndex(docMap, databaseName, collectionName, fileName);
                return fileName;
            } else {
                return "The file is not valid. Please check the schema and try again.";
            }
        }finally {
            DatabaseManager.getInstance().getDatabases().get(databaseName).getCollections().get(collectionName).getDocumentLock().writeLock().unlock();
        }
    }

    public void deleteDocument(String databaseName, String collectionName, String fileName) {
        DatabaseManager.getInstance().getDatabases().get(databaseName).getCollections().get(collectionName).getDocumentLock().writeLock().lock();
        try {
            DatabaseManager.getInstance().getDatabases().get(databaseName).getCollections().get(collectionName).getDocuments().remove(fileName);
            deleteIndex(databaseName, collectionName, fileName);

            String collectionFolderPath = System.getProperty("user.dir") + "/databases/" + databaseName + "/" + collectionName;
            File file = new File(collectionFolderPath, fileName + ".json");
            file.delete();
        } finally {
            DatabaseManager.getInstance().getDatabases().get(databaseName).getCollections().get(collectionName).getDocumentLock().writeLock().unlock();
        }
    }

    public String updateDocument(String databaseName, String collectionName, String fileName, String document) throws IOException {
        DatabaseManager.getInstance().getDatabases().get(databaseName).getCollections().get(collectionName).getDocumentLock().writeLock().lock();
        try {
            String collectionFolderPath = System.getProperty("user.dir") + "/databases/" + databaseName + "/" + collectionName;
            Map<String, String> docMap = readDocument(document, fileName);

            if(isValid(collectionFolderPath, docMap))
            {
                updateIndex(docMap, databaseName, collectionName, fileName);
                updateDocumentFile(databaseName, collectionName, docMap, fileName);
                return "Document updated successfully";
            }
            else
            {
                return "The update is not valid. Please check the schema and try again.";
            }
        }
        finally {
            DatabaseManager.getInstance().getDatabases().get(databaseName).getCollections().get(collectionName).getDocumentLock().writeLock().unlock();
        }
    }

    public boolean isValid(String collectionFolderPath, Map<String, String> docMap) throws FileNotFoundException {
        return documentValidator.isValidDocument(collectionFolderPath, docMap);
    }

    public Map<String, String> readDocument(String schema, String uuid) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Object schemaObject = mapper.readValue(schema, Object.class);
        Map<String, String> docMap = mapper.convertValue(schemaObject, Map.class);
        docMap.put("_id", uuid.toString());
        return docMap;
    }

    public void createDocumentFile(String databaseName, String collectionName, Map<String,String> docMap, String uuid) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(docMap);

        String collectionFolderPath = System.getProperty("user.dir") + "/databases/" + databaseName + "/" + collectionName;

        File file = new File(collectionFolderPath, uuid + ".json");
        file.createNewFile();
        FileWriter writer = new FileWriter(file);
        writer.write(json);
        writer.close();

        DatabaseManager.getInstance().getDatabases().get(databaseName).getCollections().get(collectionName).getDocuments().add(uuid);
    }

    private void updateDocumentFile(String databaseName, String collectionName, Map<String, String> docMap, String fileName) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(docMap);

        String collectionFolderPath = System.getProperty("user.dir") + "/databases/" + databaseName + "/" + collectionName;

        File file = new File(collectionFolderPath, fileName + ".json");

        FileWriter fileWriter = new FileWriter(file);
        fileWriter.write("");
        fileWriter.close();

        file.createNewFile();
        FileWriter writer = new FileWriter(file);
        writer.write(json);
        writer.close();
    }
    public void createIndex(Map<String, String> docMap, String databaseName, String collectionName, String uuid){
        for (String key : docMap.keySet()) {
            if (key != "_id") {
                String value = docMap.get(key);
                DatabaseManager.getInstance().getDatabases().get(databaseName).getCollections().get(collectionName).getIndexes().get(key).putIfAbsent(value, new DocumentIndex());
                DatabaseManager.getInstance().getDatabases().get(databaseName).getCollections().get(collectionName).getIndexes().get(key).get(value).getDocumentIds().add(uuid);
            }
        }
    }

    public void deleteIndex(String databaseName,String collectionName, String fileName){
        Map<String, Map<String, DocumentIndex>> map =DatabaseManager.getInstance().getDatabases().get(databaseName).getCollections().get(collectionName).getIndexes();
        for (String key : map.keySet())
        {
            for (String value : map.get(key).keySet())
            {
                map.get(key).get(value).getDocumentIds().remove(fileName);
                if(map.get(key).get(value).getDocumentIds() == null)
                    map.get(key).remove(value);
            }
        }
    }

    private void updateIndex(Map<String, String> docMap, String databaseName, String collectionName, String fileName) {
        deleteIndex(databaseName, collectionName, fileName);
        createIndex(docMap, databaseName, collectionName, fileName);
    }

    public List<String> getAllDocuments(String databaseName, String collectionName) {
        DatabaseManager.getInstance().getDatabases().get(databaseName).getCollections().get(collectionName).getDocumentLock().readLock().lock();
        try {
            return DatabaseManager.getInstance().getDatabases().get(databaseName).getCollections().get(collectionName).getDocuments();
        }finally {
            DatabaseManager.getInstance().getDatabases().get(databaseName).getCollections().get(collectionName).getDocumentLock().readLock().unlock();
        }
    }
}
