package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {


    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final String JOIN = "join";
    private static final String NEWJOIN = "newjoin";
    private static final String MESSAGE = "message";
    private static final String MESSAGE_REPLICATE = "message_replicate";
    private static final String PREDECESSOR = "predecessor";
    private static final String SUCCESSOR = "successor";
    private static final String NODE_UPDATE = "node_update";
    private static final String FULLDATA = "\"*\"";
    private static final String SELFDATA = "\"@\"";
    private static final String DELETE = "delete";
    private static final String QUERY = "query";
    private static final String RESULT = "result";
    private static final String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};
    private static final int SERVER_PORT = 10000;
    private static final int TOTAL_AVDS = 5;
    private static int deliveryCount = -1;
    private static int counter = 0;
    private static String myId;
    private static String myPort;
    private static final String URI_STRING = "edu.buffalo.cse.cse486586.simpledynamo.provider";
    private static ContentResolver mContentResolver;
    private static String ENTRY_NODE = "5554";
    private ServerTask serverTask;
    private static Context context;
    private Uri mUri;
    private static CircularLinkedList node;
    private static HashSet<ContentValues> bufferData = new HashSet<>();
    private static HashMap<String, MatrixCursor> hmResult = new HashMap<>();

    int poolSize = 10;
    int maxPoolSize = 20;
    int maxTime = 40;
    private BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(maxPoolSize);
    private Executor threadPoolExecutor = new ThreadPoolExecutor(poolSize, maxPoolSize, maxTime, TimeUnit.SECONDS, workQueue);

    private void sendJoiningMessage() {
        String[] msgToSend = {NEWJOIN, myPort};
        sendMessageToClient(msgToSend);
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        FileOutputStream fos = null;
        String fileName = selection;
        if (SELFDATA.equals(fileName)) {
            deleteSelfData();
        } else if (FULLDATA.equals(fileName)) {
            deleteSelfData();
            String originator = myPort;
            if (selectionArgs != null) {
                originator = selectionArgs[1];
            }
            String[] msgToSend = {"DELETE", node.getSuccessor(), originator, selection};
            if (!originator.equals(node.getSuccessor()))
                sendMessageToClient(msgToSend);
        } else {
            context.deleteFile(fileName);
        }
        return 0;
    }

    private void deleteSelfData() {
        String[] fileList = context.fileList();
        if (fileList != null) {
            for (String fileName : fileList) {
                context.deleteFile(fileName);
            }
            Log.v("Me Log delete", "ALL files DELETED");
        }
        else {
            Log.v("Me Log delete", "NO FILE TO DELETE");
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String key = values.getAsString(KEY_FIELD);
        boolean flag = belongToSelf(key);
        if (flag) {
            insertData(values);
            sendToSuccessorAvdForReplication(values);
        } else {
            sendToSuccessorAvd(values);
        }
        return uri;
    }

    private boolean belongToSelf(String key) {
        try {
            String hashed = genHash(key);
            return node.belongToSelf(hashed);
        } catch (NoSuchAlgorithmException ex) {
            Log.e("Me Log Error", " Error in belong To Self simpledynamo " + ex.getMessage());
        }
        return false;
    }

    private void insertData(ContentValues values) {
        // if flag is true then insert the key value in this avd only
        FileOutputStream fos = null;
        String key = values.getAsString(KEY_FIELD);
        String value = values.getAsString(VALUE_FIELD);
        try {
            fos = context.openFileOutput(key, context.MODE_PRIVATE);
            fos.write(value.getBytes());
            fos.close();
            Log.v("Me Log insert", key.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendToSuccessorAvdForReplication(ContentValues values) {
        String succPort1 = this.node.getSuccessor();
        String succPort2 = this.node.getSecondSuccessor();

        String[] msgToSendF = {MESSAGE_REPLICATE, succPort1, values.getAsString(KEY_FIELD), values.getAsString(VALUE_FIELD)};
        sendMessageToClient(msgToSendF);

        String[] msgToSendS = {MESSAGE_REPLICATE, succPort2, values.getAsString(KEY_FIELD), values.getAsString(VALUE_FIELD)};
        sendMessageToClient(msgToSendS);

        Log.v("Me Log message", "After sending insert message for replication");
    }

    private void sendToSuccessorAvd(ContentValues values) {
        String remotePort  = "";
        try{
            String hashed = genHash(values.getAsString(KEY_FIELD));
            remotePort = node.getCoordinator(hashed);
            Log.v("Me Log ", "Sending the its coordinator "+ remotePort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        String[] msgToSend = {MESSAGE, remotePort, values.getAsString(KEY_FIELD), values.getAsString(VALUE_FIELD)};
        sendMessageToClient(msgToSend);

//        Log.v("Me Log message", "AFTER SENDIn The MSG TO nEXT AVD");
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        this.context = this.getContext();
        mUri = buildUri("content", URI_STRING);

        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        this.myPort = String.valueOf((Integer.parseInt(portStr)));

        mContentResolver = context.getContentResolver();

        //Setting up the server to listen incoming messages
        setUpServerListener();

        try {
            node = new CircularLinkedList(genHash(myPort), myPort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        //sending joining message to the 5554 as
        // decided the head
        if (!node.getPort().equals(ENTRY_NODE))
            sendJoiningMessage();
        return false;
    }


    /**
     * Method to setting up server to listen
     * for any incoming message at 10000 port
     */
    private void setUpServerListener() {
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            serverTask = new ServerTask();
            serverTask.executeOnExecutor(threadPoolExecutor, serverSocket);
        } catch (IOException ex) {
            Log.e("Me Log Error", "Error in setUpServerListener " + ex.getMessage());
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        String[] columns = {KEY_FIELD, VALUE_FIELD};
        MatrixCursor cr = new MatrixCursor(columns);
        if (SELFDATA.equals(selection)) {
            cr = getSelfData();
        } else if (FULLDATA.equals(selection)) {
            cr = getSelfData();
            MatrixCursor crOther = getDataFromOtherAVD(selection, selectionArgs);
            crOther.moveToFirst();
            int keyIndex = crOther.getColumnIndex(KEY_FIELD);
            int valueIndex = crOther.getColumnIndex(VALUE_FIELD);
            Log.v("Me Log ", cr.getCount() + " Total Count before adding: ");
            do {
                if (crOther.getCount() == 0) break;
                String[] row = new String[2];
                row[0] =  crOther.getString(keyIndex);
                row[1] = crOther.getString(valueIndex);
                cr.addRow(row);
            }while (crOther.moveToNext());
            Log.v("Me Log ", cr.getCount() + " Total Count after adding: ");
            Log.v("Me Log ", crOther.getCount() + " Total other Count adding: ");
        } else {
            boolean flag = belongToSelf(selection);
            if (flag) {
                String valueContent = getValueFromKey(selection);
                if (!valueContent.isEmpty()) {
                    String[] row = new String[2];
                    row[0] = selection;
                    row[1] = valueContent;
                    cr.addRow(row);
                }
            } else {
                Log.wtf("Me Log", "querying from other node");
                cr = getDataFromOtherAVD(selection, selectionArgs);
            }
            Log.v("Me Log query", selection);
        }
        Log.v("Me Log query", "Final Result for query: " + selection + " count " + cr.getCount());
        return cr;
    }

    private MatrixCursor getSelfData() {
        String[] columns = {KEY_FIELD, VALUE_FIELD};
        MatrixCursor cr = new MatrixCursor(columns);
        String[] fileLists = context.fileList();
        if (fileLists != null) {
            for (String fileName : fileLists) {
                String valueContent = getValueFromKey(fileName);
                if (!valueContent.isEmpty()) {
                    String[] row = new String[2];
                    row[0] = fileName;
                    row[1] = valueContent;
                    cr.addRow(row);
                }
            }
            Log.v("Me Log query", "GETTING SELF DATA");
        }
        return  cr;
    }

    private String getValueFromKey(String fileName) {
        String valueContent = null;
        try{
            File fl = new File(fileName);
            FileInputStream fis = context.openFileInput(fileName);
            StringBuilder sb = new StringBuilder();
            int val = fis.read();
            while (val != -1) {
                sb.append((char) val);
                val = fis.read();
            }
            valueContent = sb.toString();
            fis.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return valueContent;
    }

    private MatrixCursor getDataFromOtherAVD(String selection, String[] selectionArgs) {
        String originPort = myPort;
        String uniqueId  = "";
        String[] columns = {KEY_FIELD, VALUE_FIELD};
        MatrixCursor mat = new MatrixCursor(columns);
        //if there is only onde node in the system
        if (FULLDATA.equals(selection)) {
            if (node.getSuccessor() == null)
                return mat;
            if (selectionArgs != null) {
                originPort = selectionArgs[0];
                uniqueId = selectionArgs[1];
            } else {
                uniqueId = myPort + counter++;
            }
            Log.v("Me Log query", "Start Query and asking data from others " + originPort + " unique ID " + uniqueId );
            try {
                if (!node.getSuccessor().equals(originPort)) {
                    String[] msgToSend = {QUERY, node.getSuccessor(), selection, originPort, uniqueId};
                    sendMessageToClient(msgToSend);
                    Log.v("Me Log ", "Waiting for data to come for selection "+ selection + " startport " + originPort);
                    while (!hmResult.containsKey(uniqueId)) {
                        Thread.sleep(100);
                    }
                    mat = hmResult.get(uniqueId);
                }
                else {
                    Log.wtf("Me Log ", " No need to get data from next node " + node.getSuccessor() + " origin " + originPort);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            if (selectionArgs != null) {
                originPort = selectionArgs[0];
                uniqueId = selectionArgs[1];
            } else {
                uniqueId = myPort + counter++;
            }
            String remotePort = null;
            try {
                remotePort = node.getCoordinator(genHash(selection));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            String[] msgToSend = {QUERY, remotePort, selection, originPort, uniqueId};
            sendMessageToClient(msgToSend);
            while (!hmResult.containsKey(uniqueId)) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            mat = hmResult.get(uniqueId);
            Log.w("Me Log ", "Got the data from other avd key: " + selection + " port " + remotePort );
        }
        return mat;
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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

    private void sendMessageToClient(String[] msgToSend) {
        AsyncTask<String, Void, Void> client =
                new ClientTask().executeOnExecutor(threadPoolExecutor, msgToSend);
    }



    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while (true) {
                try {
                    Socket clientS = serverSocket.accept();
                    Date dStart = new Date();
                    InputStreamReader is = new InputStreamReader(clientS.getInputStream());
                    BufferedReader br = new BufferedReader(is);
                    String msg = br.readLine();
                    String type = msg.split(" ")[0];
                    if (JOIN.equals(type) || NEWJOIN.equals(type)) {
                        updateRingWithNewNode(msg.split(" ")[1]);
                    } else if (NODE_UPDATE.equals(type)) {
                        //Msg contains type and hashed node id and node id of all the nodes
                        // in the circular list return by the 5554 response for joining request
                        updateNodeList(msg);
                        Log.v("Me Log Node List", node.toString());
                    } else if (MESSAGE.equals(type) || MESSAGE_REPLICATE.equals(type)) {
                        String key = msg.split(" ")[1];
                        String value = msg.split(" ")[2];
                        ContentValues cv = new ContentValues();
                        cv.put(KEY_FIELD, key);
                        cv.put(VALUE_FIELD, value);
                        //Message then treat it as normal insert
                        //other wise directly insert it as replication
                        if (MESSAGE.equals(type)) {
                            insert(mUri, cv);
                        } else if (MESSAGE_REPLICATE.equals(type)){
                            insertData(cv);
                        }
                    } else if (QUERY.equals(type)) {
                        publishProgress(msg);
                    } else if (RESULT.equals(type)) {
                        updateResultMapObject(msg);
                    } else if (DELETE.equals(type)) {
                        String[] selectionArgs = {msg.split(" ")[1]};
                        delete(mUri, msg.split(" ")[2], selectionArgs);
                    }
                    br.close();
                    is.close();
                    clientS.close();
                } catch (SocketTimeoutException ex) {
                    Log.e("Me Log Error: ", ex.getMessage() + " Server Catch Exception");
                } catch (IOException ex) {
                    Log.v("Me Log Error: ", ex.getMessage() + "  Server Catch Exception");
                }
            }
        }

        private void updateRingWithNewNode(String newNodeId) {
            try {
                String hashed = genHash(newNodeId);
                String prevSuccessor = "";
                //getting successor before updating the newNode
                // New node may become its successor
                if (node.getSuccessor() != null) {
                    prevSuccessor = node.getSuccessor();
                }

                //Adding new Node to the circular linked list
                node.addNode(hashed, newNodeId);

                //Send back reply only if its the entry node other nodes
                if (myPort.equals(ENTRY_NODE)) {
                    //Getting all the nodes hashed and port in the circular
                    //list and send to the node who just joined the system
                    String node_list = node.getAllContent();
                    String[] msgToSend= {NODE_UPDATE, newNodeId, node_list};
                    sendMessageToClient(msgToSend);
                }

                //This will stop the chain if next Node is entry node
                // as entry node only started the chain
                if (!prevSuccessor.isEmpty() && prevSuccessor.compareTo(ENTRY_NODE) != 0) {
                    String[] updateRing = {JOIN, newNodeId, prevSuccessor};
                    sendMessageToClient(updateRing);
                }

            }
            catch (NoSuchAlgorithmException ex) {
                Log.e("Me Log Error", " Error in belong To Self SimpleDhtProvider " + ex.getMessage());
            }
        }


        private void updateNodeList(String msg) {
            node = new CircularLinkedList(msg, myPort, Boolean.TRUE);
        }

        protected void onProgressUpdate(String... strings) {
            fireQueryAndReturnResults(strings[0]);
        }

        //msg format
        //msg[1] = KEY
        //msg[2] = originPort
        //msg[3] = uniqueID of the query
        private void fireQueryAndReturnResults(String msg) {
            Log.v("Me Node ", msg + " QUERY Type");
            String keyString = msg.split(" ")[1];
            String uniqueId = msg.split(" ")[3];
            String[] originPorts = {msg.split(" ")[2], uniqueId};

            Cursor cr = query(mUri, null, keyString, originPorts, null);
            int keyIndex = cr.getColumnIndex(KEY_FIELD);
            int valueIndex = cr.getColumnIndex(VALUE_FIELD);

            cr.moveToFirst();
            String keyValues = "";
            do {
                if (cr.getCount() != 0) {
                    String key = cr.getString(keyIndex);
                    String value = cr.getString(valueIndex);
                    keyValues += key + " " + value + " ";
                }
            }  while (cr.moveToNext());
            if (FULLDATA.equals(keyString)) {
                String[] msgToSend = {RESULT, node.getPredecessor(), uniqueId, keyValues.trim()};
                sendMessageToClient(msgToSend);
            } else {
               //if we are fetching only one key valye then return
               //direcctly to the rewuester
                String[] msgToSend = {RESULT, originPorts[0], uniqueId, keyValues.trim()};
                sendMessageToClient(msgToSend);
            }
        }

        private void updateResultMapObject(String msg) {
            String[] data = msg.split(" ");
            String[] columns = {KEY_FIELD, VALUE_FIELD};
            MatrixCursor cr = new MatrixCursor(columns);
            for (int i = 2; i < data.length; i+=2) {
                //to remove any space values
                if (data[i].trim().isEmpty()) i++;
                String[] row = new String[2];
                row[0] = data[i];
                row[1] = data[i + 1];
                cr.addRow(row);
            }
            String uniqueId = data[1];
            Log.v("Me Log " , "Putting the data for key " + uniqueId);
            hmResult.put(uniqueId, cr);
        }


    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... params) {
            String type = params[0];
            if (NEWJOIN.equals(type) || JOIN.equals(type)) {
                String remotePort = null;
                if (JOIN.equals(type)) {
                    remotePort = Integer.parseInt(params[2]) * 2 + "";
                }
                else if (NEWJOIN.equals(type)) {
                    remotePort = 5554 * 2 + "";
                }

                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                    String msgToSend = JOIN + " " + params[1];
                    pw.println(msgToSend);
                    pw.flush();
                    pw.close();
                    socket.close();
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                sendMessage(params);
            }
//            else if (MESSAGE.equals(type)) {
////                sendMessage(params);
//            } else if (DELETE.equals(type)) {
////                sendMessage(params);
//            } else if (QUERY.equals(type)) {
////                sendMessage(params);
//            } else if (PREDECESSOR.equals(type)) {
////                sendMessage(params);
//            } else if (SUCCESSOR.equals(type)) {
////                sendMessage(params);
//            } else if (RESULT.equals(type)) {
////                sendMessage(params);
//            }
            return null;
        }

        private void sendMessage(String[] params) {
            String msgToSend = "";
            String remotePort = "";
            if (NODE_UPDATE.equals(params[0])) {
                //0. type, 1: remote Port, 2: all the hashedkeys and port values
                remotePort = params[1];
                msgToSend = params[0] + " " + params[2];
            } else if (MESSAGE_REPLICATE.equals(params[0]) ||
                    (MESSAGE.equals(params[0]))) {
                //o. type, 1. remotePort, 2. key 3. value
                remotePort = params[1];
                msgToSend = params[0] + " " + params[2] + " " + params[3];
            } else if (QUERY.equals(params[0])) {
                //param = "type" + " " remotePort + " "  + "key" + " " + "originPort" + " " + "unqieID"
                remotePort = params[1];
                msgToSend = params[0] + " " + params[2] + " " + params[3] + " " + params[4];
            } else if (RESULT.equals(params[0])) {
                //params = "type" +" " + "remote_POrt" + " " + "uniqueID" + " " + "resultValue"
                remotePort = params[1];
                msgToSend = params[0] + " " + params[2] + " " + params[3];
            } else if (DELETE.equals(params[0])) {
                remotePort = params[1];
                msgToSend = params[0] + " " + params[2] + " " + params[3];
            }
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort) * 2);
                PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                pw.println(msgToSend.trim());
                pw.flush();
                pw.close();
                socket.close();
            } catch (SocketTimeoutException ex) {
                Log.e("Me Log Error: AVD FAILED , ", ex.getMessage());
            } catch (UnknownHostException ex) {
                Log.e("Me Log Error: AVD FAILED , ", ex.getMessage());
            } catch (IOException ex) {
                Log.e("Me Log Error: AVD FAILED , ", ex.getMessage());
            }
//            for (int i = 0; i < TOTAL_AVDS; i++) {
//                try {
//                    String remotePortHashed = genHash((Integer.parseInt(REMOTE_PORTS[i]) / 2) + "");
//                    String comparePort = node.getSuccessor();
//                    if (PREDECESSOR.equals(params[0]) || SUCCESSOR.equals(params[0])) {
//                        comparePort = genHash(params[1]);
//                    }
//                    if (RESULT.equals(params[0])) {
//                        comparePort = node.getPredecessor();
////                        comparePort = genHash("5554");
//                    }
//                    if (remotePortHashed.equals(comparePort)) {
////                        String remotePort = REMOTE_PORTS[i];
//                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                                Integer.parseInt(remotePort));
//                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
//
//                        if (MESSAGE.equals(params[0])) {
//                            msgToSend = params[0] + " " + params[1] + " " + params[2];
//                        } else if (DELETE.equals(params[0])) {
//                            msgToSend = params[0] + " " + params[2];
//                        } else if (QUERY.equals(params[0])) {
//                            //param = "type" + " " + "key" + " " + "originPort" + " " + "unqieID"
//                            msgToSend = params[0] + " " + params[1] + " " + params[2] + " " + params[3];
//                        } else if (PREDECESSOR.equals(params[0])) {
//                            //param = "type" + "predecessor ID"
//                            msgToSend = params[0] + " " + params[2];
//                        } else if (SUCCESSOR.equals(params[0])) {
//                            msgToSend = params[0] + " " + params[2];
//                        } else if (RESULT.equals(params[0])) {
//                            //params = "type" +" " + "uniqueID" + " " + "resultValue"
//                            msgToSend = params[0] + " " + params[1] + " " + params[2];
//                        }
//                        pw.println(msgToSend.trim());
//                        pw.flush();
//                        pw.close();
//                        socket.close();
//                        break;
//                    }
//                } catch (SocketTimeoutException ex) {
//                    Log.e("Error: AVD FAILED " + i + ", ", ex.getMessage());
//                } catch (UnknownHostException ex) {
//                    Log.e("Error: AVD FAILED " + i + ", ", ex.getMessage());
//                } catch (IOException ex) {
//                    Log.e("Error: AVD FAILED " + i + ", ", ex.getMessage());
//                } catch (NoSuchAlgorithmException ex) {
//                    Log.e("Error: AVD FAILED " + i + ", ", ex.getMessage());
//                    ex.printStackTrace();
//                }
        }
    }
}
