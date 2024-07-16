// JavaScript ECMAScript ECMA - 262 Edition 5.1
// JMeter JSR223 PreProcessor script

// imports
var FileUtils = org.apache.commons.io.FileUtils;
var File = java.io.File;
var StandardCharsets = java.nio.charset.StandardCharsets;
var Base64 = Java.type('java.util.Base64');
var String = Java.type('java.lang.String');
log.info("---- libraries imported ----");

// get thread number (used as userId)
var functionCall = new org.apache.jmeter.functions.ThreadNumber();
var threadNum = functionCall.execute() + 1;
log.info("---- thread #" + threadNum + " ----");

// base64 encoding function
function encodeToBase64(input) {
    var byteArray = new String(input).getBytes('UTF-8');
    var encodedString = Base64.getEncoder().encodeToString(byteArray);
    log.info("---- base64 encoded string: \"" + encodedString + "\" ----");
    encodedString = new String(encodedString).replace(/=+$/, '');
    log.info("---- base64 encoded string (trimmed): \"" + encodedString + "\" ----");
    return encodedString;
}

// load data (as base64 string / bytes)
var path = "/Users/qobiljon/Desktop/jmeter/data/1MB.txt";
var dataString = FileUtils.readFileToString(new File(path), StandardCharsets.UTF_8);
// var dataBase64 = toBase64(data);
var dataBase64 = encodeToBase64(dataString);
log.info("---- data loaded ----");

// prepare variables
var userId = threadNum;
var sessionKey = "participant" + userId;
var campaignId = 0;
var timestamp = new Date().getTime();
var dataSourceId = 0;
var accuracy = 12345.6789;
var value = dataBase64;
log.info("---- variables set ----");

// pass variables to sampler (values for JSON request body)
vars.put("userId", 24);
vars.put("sessionKey", "participant24");
vars.put("campaignId", campaignId);
vars.put("timestamp", timestamp);
vars.put("dataSource", dataSourceId);
vars.put("accuracy", accuracy);
vars.put("value", value);
log.info("---- variables passed to sampler ----");