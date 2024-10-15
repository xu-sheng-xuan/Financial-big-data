package com.example.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class StopWordList {
    public Set<String> getStopWords() throws IOException {
        Set<String> stopWords = new HashSet<>();
        BufferedReader reader = new BufferedReader(new FileReader("stop-word-list.txt"));
        String line;
        while ((line = reader.readLine()) != null) {
            stopWords.add(line.trim().toLowerCase());
        }
        reader.close();
        return stopWords;
    }
}