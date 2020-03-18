package com.phonepe.fabric.foxtrot.ingestion.utils;

import com.flipkart.foxtrot.client.Document;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

    private Utils() {
    }

    private static final String ERRONEOUS_PATTERN = "\u0000";

    public static String getSampleDocument(List<Document> documents) {
        return documents.isEmpty()
                ? "N/A" : documents.get(0).getData() == null
                ? "N/A" : documents.get(0).getData().toString();
    }

    public static boolean isErroneousAppName(String appName) {
        Pattern pattern = Pattern.compile(ERRONEOUS_PATTERN);
        Matcher matcher = pattern.matcher(appName);
        return matcher.find();
    }

    public static String sanitizeAppName(String appName) {
        Pattern pattern = Pattern.compile(ERRONEOUS_PATTERN);
        Matcher matcher = pattern.matcher(appName);
        while (matcher.find()) {
            String erroneous = matcher.group();
            appName = appName.replace(erroneous, "");
        }
        return appName;
    }

}
