package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Message implements Serializable {
    int msgType;
    String portNumber;
    String hashedPortNumber;
    String predPortNumber;
    String hashedPredPortNumber;
    String succPortNumber;
    String hashedSuccPortNumber;
    String key;
    String hashedKey;
    String value;
    Map<String, String> files = new HashMap<String, String>();

    public Message(int msgType, String portNumber, String hashedPortNumber,String predPortNumber, String hashedPredPortNumber, String succPortNumber, String hashedSuccPortNumber) {
        this.msgType = msgType;
        this.portNumber = portNumber;
        this.hashedPortNumber = hashedPortNumber;
        this.predPortNumber = predPortNumber;
        this.hashedPredPortNumber = hashedPredPortNumber;
        this.succPortNumber = succPortNumber;
        this.hashedSuccPortNumber = hashedSuccPortNumber;
    }

    public Message(int msgType, String key, String hashedKey, String value) {
        this.msgType = msgType;
        this.key = key;
        this.hashedKey = hashedKey;
        this.value = value;
    }

    public Message(int msgType, String portNumber, Map<String, String> files) {
        this.msgType = msgType;
        this.portNumber = portNumber;
        this.files = files;
    }

    public Message(int msgType, String portNumber, String key) {
        this.msgType = msgType;
        this.portNumber = portNumber;
        this.key = key;
    }

    public Message(int msgType, Map<String, String> files) {
        this.msgType = msgType;
        this.files = files;
    }
}
