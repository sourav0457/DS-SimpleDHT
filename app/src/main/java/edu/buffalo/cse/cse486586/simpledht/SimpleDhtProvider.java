package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import android.annotation.SuppressLint;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

import static edu.buffalo.cse.cse486586.simpledht.SimpleDhtActivity.myPort;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    public String predPort = null;             // e.g. 11108
    String predPortHash = null;         // e.g. jkjdhfkhdf (for 5554)
    String succPort = null;             // e.g. 11112
    String succPortHash = null;         // e.g. dksjdhfsjdf (for 5556)
    String initNodePort = "11108";
    String selfNodeId = null;           // e.g. 5558
    String selfPort = null;             // e.g. 11118
    String selfPortHash = null;         // e.g. jkdhfkjshdfiu (for 5558)
    static final int SERVER_PORT = 10000;
    static ArrayList<String> keysSaved = new ArrayList<String>();
    static Map<String, String> fileData = new HashMap<String, String>();
    boolean flag = false;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if(selection.equals("@")) {
            keysSaved.clear();
        }
        else if(keysSaved.contains(selection)){
            keysSaved.remove(selection);
        }
        else {
            String msg = "3";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, selection);
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        ArrayList<String> retrievedValues = new ArrayList<String>();
        Set<Map.Entry<String, Object>> entries = values.valueSet();
        for(Map.Entry<String, Object> entry: entries) {
            retrievedValues.add(entry.getValue().toString());
        }
        Log.d("insert", "value: " + retrievedValues.get(0) + ", key: " + retrievedValues.get(1));
        Log.v("insert", values.toString());

        String msgId = "1"; // 1- Represents insert for Client
        String key = retrievedValues.get(1);
        String value = retrievedValues.get(0);
        String keyHash = null;
        try {
            keyHash = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgId, key, keyHash, value);

//        String fileName = retrievedValues.get(1)+".txt";
//        try {
//            String key = genHash(fileName);
//        } catch (NoSuchAlgorithmException e) {
//            e.printStackTrace();
//        }
//        String valueToStore = retrievedValues.get(0);
//        Context context = getContext();
//
//        try {
//            Log.d("Saving Files: ", "Creating file");
//            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(context.openFileOutput(fileName, Context.MODE_PRIVATE));
//            outputStreamWriter.write(valueToStore);
//            outputStreamWriter.close();
//            Log.d("Saving Files: ", "Successfully written to file");
//        }
//        catch(IOException e) {
//            Log.e("Exception", "File Write Failed");
//        }

        return uri;
    }

    public void insertFile(String key, String value) {

        Context context = getContext();
        String fileName = key + ".txt";
        try {
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(context.openFileOutput(fileName, Context.MODE_PRIVATE));
            outputStreamWriter.write(value);
            outputStreamWriter.close();
            keysSaved.add(key);
            Log.d("Saving Files: ", "Successfully written to file: " + key + " to Node with Port: " + selfPort);
        } catch (IOException e) {
            Log.e("Exception", "File Write Failed");
            e.printStackTrace();
        }
    }

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        @SuppressLint("MissingPermission") String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        Log.d("Custom", "port number base: "+portStr);

        selfNodeId = portStr;
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        selfPort = myPort;
        try {
            selfPortHash = genHash(selfNodeId);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if(!selfNodeId.equals("5554")) {
            String msg = "0";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
        }

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        String[] columns = new String[]{"key", "value"};
        MatrixCursor cur = new MatrixCursor(columns);
        if(selection.equals("@")){
            String[] retrievedValues;
            for(String key: keysSaved) {
                try {
                    String valueRead = "";
                    InputStream inputStream = getContext().openFileInput(key+".txt");
                    if(inputStream != null) {
                        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                        valueRead = bufferedReader.readLine();
                        inputStream.close();
                        Log.d("Read => ", valueRead);
                        retrievedValues = new String[]{key, valueRead};
                        cur.addRow(retrievedValues);
                    }
                }
                catch (FileNotFoundException e){
                    Log.e("Read ", "File " + key + " not found!: " + e.toString());
                }
                catch (IOException e) {
                    Log.e("Read ", "Cannot read file: " + e.toString());
                }
            }
        }
        else if(keysSaved.contains(selection)) {
            try {
                String[] retrievedValues;
                String valueRead = "";
                InputStream inputStream = getContext().openFileInput(selection+".txt");
                if(inputStream != null) {
                    InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    valueRead = bufferedReader.readLine();
                    inputStream.close();
                    Log.d("Read => ", valueRead);
                    retrievedValues = new String[]{selection, valueRead};
                    cur.addRow(retrievedValues);
                }
            }
            catch (FileNotFoundException e){
                Log.e("Read ", "File " + selection + " not found!: " + e.toString());
            }
            catch (IOException e) {
                Log.e("Read ", "Cannot read file: " + e.toString());
            }
        }
        else {
            // Case: Either key is not present in the current server, or key is *
            String msg = "2";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, selection);
            // Put a while loop to wait for server to update the hashmap from all the nodes
            flag = true;
            while(flag);    // Continue to loop until flag is set to false

            String[] retrievedValues;
            // Once flag is set to false, extract values from the hashmap and construct the cursor object which will be returned
            for(Map.Entry<String, String> entry : fileData.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                retrievedValues = new String[]{key, value};
                cur.addRow(retrievedValues);
            }
        }
        return cur;
    }

    public void queryFile(String key) {
        fileData.clear();
        if(key.equals("*")){
            for(String file: keysSaved) {
                try {
                    String valueRead = "";
                    InputStream inputStream = getContext().openFileInput(file+".txt");
                    if(inputStream != null) {
                        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                        valueRead = bufferedReader.readLine();
                        inputStream.close();
                        Log.d("Read => ", valueRead);
                        fileData.put(file, valueRead);
                    }
                }
                catch (FileNotFoundException e){
                    Log.e("Read ", "File " + key + " not found!: " + e.toString());
                }
                catch (IOException e) {
                    Log.e("Read ", "Cannot read file: " + e.toString());
                }
            }
        }
        else {
            try {
                String valueRead = "";
                InputStream inputStream = getContext().openFileInput(key+".txt");
                if(inputStream != null) {
                    InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    valueRead = bufferedReader.readLine();
                    inputStream.close();
                    Log.d("Read => ", valueRead);
                    fileData.put(key, valueRead);
                }
            }
            catch (FileNotFoundException e){
                Log.e("Read ", "File " + key + " not found!: " + e.toString());
            }
            catch (IOException e) {
                Log.e("Read ", "Cannot read file: " + e.toString());
            }
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    public class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {                                        // Since only one parameter was passed (socket instance), sockets[0] contains the socket instance
            ServerSocket serverSocket = sockets[0];
            try {
                while(true){
                    Log.d(TAG, "Predecessor: " + predPort + " Successor: " + succPort);
                    Socket socket = serverSocket.accept();
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    Message objectRead = (Message) ois.readObject();

                    if(objectRead.msgType == 1) {
                        // Only for port 5554
                        Log.d(TAG, "5554: Adding " + objectRead.portNumber + " to chord.");
                        if(predPort == null && succPort == null) {
                            succPort = objectRead.portNumber;
                            predPort = objectRead.portNumber;
                            succPortHash = objectRead.hashedPortNumber;
                            predPortHash = objectRead.hashedPortNumber;
                            Log.d(TAG, "MT = 1, Condition: (null, null) PORT: " + selfNodeId + " Adding " + objectRead.portNumber + " to chord. Predecessor: " + predPort + " Successor: " + succPort);
                            Message objectToSend = new Message(2, null, null, selfPort, selfPortHash, selfPort, selfPortHash);
                            Socket replySocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(objectRead.portNumber));
                            ObjectOutputStream out = new ObjectOutputStream(replySocket.getOutputStream());
                            out.writeObject(objectToSend);
                            Log.d(TAG, "MT = 1, Condition: (null, null) Successfully Added " + objectRead.portNumber + " to chord. Predecessor: " + predPort + " Successor: " + succPort);
                        }
                        else if((objectRead.hashedPortNumber.compareTo(selfPortHash) > 0 && objectRead.hashedPortNumber.compareTo(succPortHash) < 0) ||
                                (objectRead.hashedPortNumber.compareTo(selfPortHash) > 0 && selfPortHash.compareTo(succPortHash) > 0) ||
                                (objectRead.hashedPortNumber.compareTo(selfPortHash) < 0 && objectRead.hashedPortNumber.compareTo(succPortHash) < 0 && selfPortHash.compareTo(succPortHash) > 0)) {

                            Log.d(TAG, "MT = 1, Condition: (> self, < succ || succ < self) Adding " + objectRead.portNumber + " to chord. Predecessor: " + predPort + " Successor: " + succPort);
                            // Message to be sent to the node that is joining
                            Message msgToSend1 = new Message(2, null, null, selfPort, selfPortHash, succPort, succPortHash);
                            Socket joiningNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(objectRead.portNumber));
                            ObjectOutputStream out1 = new ObjectOutputStream(joiningNode.getOutputStream());
                            out1.writeObject(msgToSend1);

                            Log.d(TAG, "MT = 1, Condition: (> self, < succ || succ < self) Type 2 message sent to " + objectRead.portNumber + ". Predecessor: " + predPort + " Successor: " + succPort);
                            // Message to be sent to successor port
                            Message msgToSend2 = new Message(3, null, null, objectRead.portNumber, objectRead.hashedPortNumber, null, null);
                            Socket succNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succPort));
                            ObjectOutputStream out2 = new ObjectOutputStream(succNode.getOutputStream());
                            out2.writeObject(msgToSend2);
                            Log.d(TAG, "MT = 1, Condition: (> self, < succ || succ < self) Type 3 message sent to " + objectRead.portNumber + ". Predecessor: " + predPort + " Successor: " + succPort);

                            succPort = objectRead.portNumber;
                            succPortHash = objectRead.hashedPortNumber;
                            Log.d(TAG, "MT = 1, Condition: (> self, < succ || succ < self) Successor changed. Predecessor: " + predPort + " Successor: " + succPort);
                        }
                        else if(objectRead.hashedPortNumber.compareTo(selfPortHash) != 0) {

                            Log.d(TAG, "MT = 1, Condition: (> self) Adding " + objectRead.portNumber + " to chord. Predecessor: " + predPort + " Successor: " + succPort);
                            // Message to be sent to successor port
                            Message msgToSend = new Message(4, objectRead.portNumber, objectRead.hashedPortNumber, null, null, null, null);
                            Socket succNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succPort));
                            ObjectOutputStream out = new ObjectOutputStream(succNode.getOutputStream());
                            out.writeObject(msgToSend);
                            Log.d(TAG, "MT = 1, Condition: (> self). Successfully forwarded " + objectRead.portNumber + " request. Predecessor: " + predPort + " Successor: " + succPort);
                        }
                        else {
                            Log.e("ERROR - 1","THIS SHOULD NOT BE WORKING");
                        }
                    }
                    else if(objectRead.msgType == 2) {
                        predPort = objectRead.predPortNumber;
                        predPortHash = objectRead.hashedPredPortNumber;
                        succPort = objectRead.succPortNumber;
                        succPortHash = objectRead.hashedSuccPortNumber;
                        Log.d(TAG, "Got added to chord: Predecessor: " + predPort + " Successor: " + succPort);
                    }
                    else if(objectRead.msgType == 3) {
                        predPort = objectRead.predPortNumber;
                        predPortHash = objectRead.hashedPredPortNumber;
                        Log.d(TAG, "Predecessor Value Changed: Predecessor: " + predPort + " Successor: " + succPort);
                    }
                    else if(objectRead.msgType == 4) {
                        Log.d(TAG, "MT - 4: Current Object Port: " + selfPort + " Current Object Port Hash: " + selfPortHash + " New Node Port: " + objectRead.portNumber + " Hashed Port: " + objectRead.hashedPortNumber);
                        if((objectRead.hashedPortNumber.compareTo(selfPortHash) > 0 && objectRead.hashedPortNumber.compareTo(succPortHash) < 0) ||
                           (objectRead.hashedPortNumber.compareTo(selfPortHash) > 0 && selfPortHash.compareTo(succPortHash) > 0) ||
                           (objectRead.hashedPortNumber.compareTo(selfPortHash) < 0 && objectRead.hashedPortNumber.compareTo(succPortHash) < 0 && selfPortHash.compareTo(succPortHash) > 0)) {
                            // Message to be sent to the node that is joining
                            Log.d(TAG, "MT-4: Condition - 1: Current Port: " + selfPort + " Curent Port Hash: " + selfPortHash + " New Node: " + objectRead.portNumber + " New Node Hash: " + objectRead.hashedPortNumber + " Pred Node: " + predPort + " Pred Port Hash: " + predPortHash + " Succ Port: " + succPort + " Succ Port Hash: " + succPortHash);
                            Message msgToSend1 = new Message(2, null, null, selfPort, selfPortHash, succPort, succPortHash);
                            Socket joiningNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(objectRead.portNumber));
                            ObjectOutputStream out1 = new ObjectOutputStream(joiningNode.getOutputStream());
                            out1.writeObject(msgToSend1);
                            Log.d(TAG, "MT-4: Condition - 1: msgToSend1: Succ Node: " + succPort + " Pred Node: " + selfPort + " for Port: " + objectRead.portNumber);
                            // Message to be sent to successor port
                            Message msgToSend2 = new Message(3, null, null, objectRead.portNumber, objectRead.hashedPortNumber, null, null);
                            Socket succNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succPort));
                            ObjectOutputStream out2 = new ObjectOutputStream(succNode.getOutputStream());
                            out2.writeObject(msgToSend2);
                            Log.d(TAG, "MT-4: Condition - 1: msgToSend2: Pred Node: " + objectRead.portNumber + " for Port: " + succPort);

                            succPort = objectRead.portNumber;
                            succPortHash = objectRead.hashedPortNumber;
                            Log.d(TAG, "MT-4: Condition - 1: Final Block: Succ Node: " + succPort + " Pred Node: " + predPort + " for Port: " + selfPort);
                        }
                        else if(objectRead.hashedPortNumber.compareTo(selfPortHash) != 0) {
                            Log.d(TAG, "MT - 4: Condition - 2: Current Object Port: " + objectRead.portNumber + " Hashed Port: " + objectRead.hashedPortNumber);
                            // Message to be sent to successor port
                            Message msgToSend = new Message(4, objectRead.portNumber, objectRead.hashedPortNumber, null, null, null, null);
                            Socket succNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succPort));
                            ObjectOutputStream out = new ObjectOutputStream(succNode.getOutputStream());
                            out.writeObject(msgToSend);
                            Log.d(TAG, "MT-4: Condition - 2: Final Block: Succ Node: " + succPort + " Pred Node: " + predPort + " for Port: " + selfPort);
                        }
                        else {
                            Log.e("ERROR - 1","THIS SHOULD NOT BE WORKING");
                        }
                    }
                    else if(objectRead.msgType == 12) {
                        // Insert message into self
                        insertFile(objectRead.key, objectRead.value);
                    }
                    else if(objectRead.msgType == 11) {
                        Log.d(TAG, "Insert Message: Received Key: " + objectRead.key + " Received Key Hash: " + objectRead.hashedKey + " Current Node: " + selfPort + " Succ port: " + succPort);
                        if(predPort == null && succPort == null) {
                            Log.d(TAG, "Predecessor and Successor are Null. Inserting to self");
                            insertFile(objectRead.key, objectRead.value);
                        }
                        else if((objectRead.hashedKey.compareTo(selfPortHash) > 0 && objectRead.hashedKey.compareTo(succPortHash) < 0) ||
                           (objectRead.hashedKey.compareTo(selfPortHash) > 0 && selfPortHash.compareTo(succPortHash) > 0) ||
                           (objectRead.hashedKey.compareTo(selfPortHash) < 0 && objectRead.hashedKey.compareTo(succPortHash) < 0 && selfPortHash.compareTo(succPortHash) > 0)) {
                            // Insert should be done in successor node
                            Log.d(TAG, "Insert Message: Received Key: " + objectRead.key + " Received Key Hash: " + objectRead.hashedKey + " Getting inserted in Successor node: " + succPort);
                            Message insertMsg = new Message(12, objectRead.key, objectRead.hashedKey, objectRead.value);
                            Socket succNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succPort));
                            ObjectOutputStream out = new ObjectOutputStream(succNode.getOutputStream());
                            out.writeObject(insertMsg);
                        }
                        else if(objectRead.hashedKey.compareTo(selfPortHash) != 0) {
                            Log.d(TAG, "Insert Message: Received Key: " + objectRead.key + " Received Key Hash: " + objectRead.hashedKey + " Forwarding Message to Successor node: " + succPort);
                            Socket succNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succPort));
                            ObjectOutputStream out = new ObjectOutputStream(succNode.getOutputStream());
                            out.writeObject(objectRead);
                        }
                    }
                    else if(objectRead.msgType == 22) {
                        if(keysSaved.contains(objectRead.key)) {
                            // Call Query Function with particular key which should populate hash table from where the value will be read
                            queryFile(objectRead.key);
                            // Return the value stored in the hashmap
                            Message queryMessage = new Message(23, fileData);
                            Socket origNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(objectRead.portNumber));
                            ObjectOutputStream out = new ObjectOutputStream(origNode.getOutputStream());
                            out.writeObject(queryMessage);
                        }
                        else {
                            Socket succNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succPort));
                            ObjectOutputStream out = new ObjectOutputStream(succNode.getOutputStream());
                            out.writeObject(objectRead);
                        }
                    }
                    else if(objectRead.msgType == 21) {
                        // Star
                        queryFile("*");
                        objectRead.files.putAll(fileData);
                        if(succPort == null) {
                            flag = false;
                        }
                        else if(succPort.equals(objectRead.portNumber)){
                            Message queryMessage = new Message(23, objectRead.files);
                            Socket origNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(objectRead.portNumber));
                            ObjectOutputStream out = new ObjectOutputStream(origNode.getOutputStream());
                            out.writeObject(queryMessage);
                        }
                        else {
                            Socket succNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succPort));
                            ObjectOutputStream out = new ObjectOutputStream(succNode.getOutputStream());
                            out.writeObject(objectRead);
                        }
                    }
                    else if(objectRead.msgType == 23) {
                        fileData = objectRead.files;
                        flag = false;
                    }
                    else if(objectRead.msgType == 31) {
                        // Delete current and pass on
                        keysSaved.clear();
                        if(!succPort.equals(objectRead.portNumber)){
                            Socket succNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succPort));
                            ObjectOutputStream out = new ObjectOutputStream(succNode.getOutputStream());
                            out.writeObject(objectRead);
                        }
                    }
                    else if(objectRead.msgType == 32) {
                        // If key is present in the current AVD, delete it, otherwise pass on
                        if(keysSaved.contains(objectRead.key)){
                            keysSaved.remove(objectRead.key);
                        }
                        else if(!succPort.equals(objectRead.portNumber)) {
                            Socket succNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succPort));
                            ObjectOutputStream out = new ObjectOutputStream(succNode.getOutputStream());
                            out.writeObject(objectRead);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    public class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            Log.d(TAG, "Inside Client task. SelfPort: " + selfPort + " SelfNodeId: " + selfNodeId + " selfPortHash: " + selfPortHash);
            if(msgs[0].equals("0")) {
                Message initMsg = new Message(1, selfPort, selfPortHash, null, null, null, null);
                Log.d(TAG, "Inside Client task. Message Prepared");
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(initNodePort));
                    Log.d(TAG, "Inside Client Task. Socket created successfully: " + socket);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    Log.d(TAG, "Inside Client Task. Successfully created out object");
                    out.writeObject(initMsg);
                    Log.d(TAG, "Node join message sent from port " + selfPort);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if(msgs[0].equals("1")) {
                Message insertMsg = new Message(11, msgs[1], msgs[2], msgs[3]);
                Log.d(TAG, "Insert Message. Message Prepared.");
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(selfPort));
                    Log.d(TAG, "Inside Client Task. Socket created successfully: " + socket);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    Log.d(TAG, "Inside Client Task. Successfully created out object");
                    out.writeObject(insertMsg);
                    Log.d(TAG, "Insert message sent from port " + selfPort + " to server.");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if(msgs[0].equals("2")) {
                Message query;
                if(msgs[1].equals("*")) {
                    Map<String, String> fileInfo = new HashMap<String, String>();
                    Log.d(TAG, "Query Message. Type: *");
                    query = new Message(21, selfPort, fileInfo);
                }
                else {
                    query = new Message(22, selfPort, msgs[1]);
                    Log.d(TAG, "Query Message. Type: Selection. Key Value: " + msgs[1]);
                }
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(selfPort));
                    Log.d(TAG, "Inside Client Task. Socket created successfully: " + socket);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    Log.d(TAG, "Inside Client Task. Successfully created out object");
                    out.writeObject(query);
                    Log.d(TAG, "Query message sent from port " + selfPort + " to server.");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if(msgs[0].equals("3")){
                Message delete;
                if(msgs[1].equals("*")) {
                    delete = new Message(31, selfPort, "*");
                    Log.d(TAG, "Delete Message. Type: *");
                }
                else {
                    delete = new Message(32, selfPort, msgs[1]);
                    Log.d(TAG, "Delete Message. Type: Selection. Key Value: " + msgs[1]);
                }
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(selfPort));
                    Log.d(TAG, "Inside Client Task. Socket created successfully: " + socket);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    Log.d(TAG, "Inside Client Task. Successfully created out object");
                    out.writeObject(delete);
                    Log.d(TAG, "Delete message sent from port " + selfPort + " to server.");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }


    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
