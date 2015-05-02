package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    private static final String LOST_MESSAGE = "lost_message";
    private static final String MESSAGE_REPLICATE = "message_replicate";
    private static final String PREDECESSOR = "predecessor";
    private static final String SUCCESSOR = "successor";
    private static final String NODE_UPDATE = "node_update";
    private static final String ACK_INSERT = "ack_insert";
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
    private static int insert_counter = 0;
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
    private static HashMap<String, Integer> hmResult = new HashMap<>();
    private static HashMap<String, MatrixCursor> hmCursor = new HashMap<>();
    private static HashMap<String, Lock> hmLock = new HashMap<>();
    private static HashMap<String, Integer> hmResult_Insert = new HashMap<>();
    private static BlockingQueue<Boolean> blocking_queue;
    private static Boolean init_Lock = false;
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
        } else {
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
        Log.v("Me Log", "On insert starting init_lock : " + init_Lock);
        Lock lock = hmLock.get(myPort);
        String key = values.getAsString(KEY_FIELD);
        Log.w("Me Log insert", "aquring Global lock for new key lock " + key);
        lock.lock();
        Lock lock_key = hmLock.get(key);
        if (lock_key == null) {
            lock_key = new ReentrantLock();
            hmLock.put(key, lock_key);
        }
        lock_key.lock();
        lock.unlock();
        Log.w("Me Log insert", "unlock for new key lock " + key);
        insert_counter++;
        String id = myPort + insert_counter;
        boolean flag = belongToSelf(key);
        if (flag) {
            insertData(values);
            hmResult_Insert.put(id, 1);
        } else {
            sendToSuccessorAvd(values, id);
        }
        sendToSuccessorAvdForReplication(values, id);
        while (!hmResult_Insert.containsKey(id) || hmResult_Insert.get(id) < 2) {
            try {
                Thread.sleep(10);
                Log.w("Me Log insert", "waiting inside while loop ");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        lock_key.unlock();
        Log.w("Me Log insert", "unlock for Actual key lock " + key);
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

    private boolean belongToPredecessor(String key) {
        try {
            String hashed = genHash(key);
            return node.belongToPredecessor(hashed);
        } catch (NoSuchAlgorithmException ex) {
            Log.e("Me Log Error", " Error in belong To PREDECESSOR simpledynamo " + ex.getMessage());
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

    private void sendToSuccessorAvdForReplication(ContentValues values, String id) {
        String key = "";
        try {
            key = genHash(values.getAsString(KEY_FIELD));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

//        String succPort1 = this.node.getSuccessor();
//        String succPort2 = this.node.getSecondSuccessor();
        String[] succPort = this.node.getKeySuccessor(key);
        String[] msgToSendF = {MESSAGE_REPLICATE, succPort[0], values.getAsString(KEY_FIELD),
                values.getAsString(VALUE_FIELD), myPort, id};
        sendMessageToClient(msgToSendF);

        String[] msgToSendS = {MESSAGE_REPLICATE, succPort[1],
                values.getAsString(KEY_FIELD), values.getAsString(VALUE_FIELD), myPort, id};
        sendMessageToClient(msgToSendS);

        Log.v("Me Log message", "After sending insert message for replication " + succPort[0] + " s " + succPort[1]);
    }

    private void sendToSuccessorAvd(ContentValues values, String id) {
        String remotePort = "";
        try {
            String hashed = genHash(values.getAsString(KEY_FIELD));
            remotePort = node.getCoordinator(hashed);
            Log.v("Me Log ", "Sending the its coordinator " + remotePort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        String[] msgToSend = {MESSAGE, remotePort, values.getAsString(KEY_FIELD),
                values.getAsString(VALUE_FIELD), myPort, id};
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
        String[] ports = {"5558", "5560", "5562", "5556", "5554"};
        try {
            String msg_ports = "TEMP ";//No use
            for (int i = 0; i < ports.length; i++) {
                msg_ports += genHash(ports[i]) + " " + ports[i] + " ";
            }
            node = new CircularLinkedList(msg_ports.trim(), myPort, Boolean.TRUE);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        //Delete Self Data In starting the Application
        deleteSelfData();
        Log.v("Me Log1 ", "!!!!" + node.toString());
        //old logic sending joining message to the 5554 as
        //decided the head
        sendJoiningMessage();
        Lock lock = new ReentrantLock();
        hmLock.put(myPort, lock);
        init_Lock = false;
        //lock.lock();

        Log.v("Me Log", "On Create After applying Lock");
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
        Log.v("Me Log", "On query starting init_lock : " + init_Lock);

        String key = selection;
        Log.w("Me Log query", "Try to get global lock for key lock " + key);
        Lock lock = hmLock.get(myPort);
        lock.lock();
        Log.w("Me Log query", "Got the Global Lock for " + key);
        Lock lock_key = hmLock.get(key);
        if (lock_key == null) {
            lock_key = new ReentrantLock();
            hmLock.put(key, lock_key);
        }
        lock_key.lock();
        Log.w("Me Log query", "Got the query key Lock for " + key);
        lock.unlock();
        Log.w("Me Log query", "unlock the global lock for key lock " + key);
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
                row[0] = crOther.getString(keyIndex);
                row[1] = crOther.getString(valueIndex);
                cr.addRow(row);
            } while (crOther.moveToNext());
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
                cr = getDataFromOtherAVD(selection, selectionArgs);
                Log.wtf("Me Log", "querying from other node " );
                if (cr == null) {
                    Log.wtf("Me Log", "~~~~~~~~cr is null " );
                }
            }
            Log.v("Me Log query", selection);
        }
        Log.v("Me Log query", "Final Result for query: " + selection + " count " + cr.getCount());
        lock_key.unlock();
        Log.w("Me Log query", "unlock for Actual key lock " + key);
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
        return cr;
    }

    private String getValueFromKey(String fileName) {
        String valueContent = "";
        try {
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

    private synchronized String[] getPortAndUniqueId(String[] selectionArgs) {
        String[] result = new String[2];
        if (selectionArgs != null) {
            result[0] = selectionArgs[0];
            result[1] = selectionArgs[1];
        } else {
            result[0] = myPort;
            result[1] = myPort + counter++;
        }
        return result;
    }

    private MatrixCursor getDataFromOtherAVD(String selection, String[] selectionArgs) {
        String originPort = myPort;
        String uniqueId = "";
        String[] columns = {KEY_FIELD, VALUE_FIELD};
        MatrixCursor mat = new MatrixCursor(columns);
        //if there is only onde node in the system
        if (FULLDATA.equals(selection)) {
            if (node.getSuccessor() == null)
                return mat;

            String[] portAndUnique = getPortAndUniqueId(selectionArgs);
            originPort = portAndUnique[0];
            uniqueId = portAndUnique[1];

            try {
                if (!node.getSuccessor().equals(originPort)) {
                    String[] msgToSend = {QUERY, node.getSuccessor(), selection, originPort, uniqueId};
                    sendMessageToClient(msgToSend);
                    Log.v("Me Log ", "Waiting for data to come for selection " + selection + " start port " + originPort);
                    while (!hmResult.containsKey(uniqueId) || hmResult.get(uniqueId) < REMOTE_PORTS.length - 1) {
                        Thread.sleep(10);
                        Log.v("Me Log ", "waiting Data from other avd ");
                    }
                    mat = hmCursor.get(uniqueId);
                    hmCursor.remove(uniqueId);
                    hmResult.remove(uniqueId);
                } else {
                    Log.wtf("Me Log ", " No need to get data from next node " + node.getSuccessor() + " origin " + originPort);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            String[] portAndUnique = getPortAndUniqueId(selectionArgs);
            originPort = portAndUnique[0];
            uniqueId = portAndUnique[1];

            String remotePort = null;
            String[] remoteNextPort = new String[2];
            try {
                remotePort = node.getCoordinator(genHash(selection));
                remoteNextPort = node.getKeySuccessor(genHash(selection));

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            String[] msgToSend = {QUERY, remotePort, selection, originPort, uniqueId, remoteNextPort[0], remoteNextPort[1]};
            sendMessageToClient(msgToSend);

            while (!hmResult.containsKey(uniqueId)) {
                try {
                    Thread.sleep(10);
                    Log.v("Me Log", "Waiting for data from other avd");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            mat = hmCursor.get(uniqueId);
            Log.w("Me Log ", "Got the data from other avd key: " + selection + " port " + remotePort);
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
                    InputStreamReader is = new InputStreamReader(clientS.getInputStream());
//                    BufferedReader br = new BufferedReader(is);
                    ObjectInputStream ois = new ObjectInputStream(clientS.getInputStream());
//                    String msg = br.readLine();
                    String msg = (String)ois.readObject();
                    Log.v("Me Log1 ", "Server Message " + msg);

                    ObjectOutputStream oos = new ObjectOutputStream(clientS.getOutputStream());
                    oos.writeObject(new String("ACK"));
                    oos.flush();
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
                        String originator_port = msg.split(" ")[3];
                        String insert_id = msg.split(" ")[4];
                        ContentValues cv = new ContentValues();
                        cv.put(KEY_FIELD, key);
                        cv.put(VALUE_FIELD, value);
                        //Message then treat it as normal insert
                        //other wise directly insert it as replication
                        if (MESSAGE.equals(type)) {
                            insertData(cv);
                            //if message came from client task it means that
                            // it belongs to me only
//                            insert(mUri, cv);
                        } else if (MESSAGE_REPLICATE.equals(type)) {
                            insertData(cv);
                        }
                        sendAckToOriginator(originator_port, insert_id);
                    } else if (QUERY.equals(type)) {
                        publishProgress(msg);
                    } else if (RESULT.equals(type)) {
                        updateResultMapObject(msg);
                    } else if (DELETE.equals(type)) {
                        String[] selectionArgs = {msg.split(" ")[1]};
                        delete(mUri, msg.split(" ")[2], selectionArgs);
                    } else if (LOST_MESSAGE.equals(type)) {
                        recoverLostMessages(msg);
                    } else if (ACK_INSERT.equals(type)) {
                        String id = msg.split(" ")[1];
                        if (hmResult_Insert.containsKey(id)) {
                            hmResult_Insert.put(id, hmResult_Insert.get(id) + 1);
                        } else {
                            hmResult_Insert.put(id, 1);
                        }
                        Log.v("Me Log ", "Received Ack Insertion id: " + id );

                    }

                    oos.close();
                    ois.close();
                    clientS.close();
                } catch (SocketTimeoutException ex) {
                    Log.e("Me Log Error: ", ex.getMessage() + " Server Catch Exception");
                } catch (IOException ex) {
                    Log.v("Me Log Error: ", ex.getMessage() + "  Server Catch Exception");
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }

        private void sendAckToOriginator(String originator_port, String id) {
            String[] msgToSend = {ACK_INSERT, originator_port, id};
            sendMessageToClient(msgToSend);
            Log.v("Me Log ", "SEnding Ack Insertion id: " + id + " Port: " + originator_port);
        }

        private void updateNodeList(String msg) {
            node = new CircularLinkedList(msg, myPort, Boolean.TRUE);
        }

        private void recoverLostMessages(String msg) {
            String[] data = msg.split(" ");
            for (int i = 1; i < data.length; i += 2) {
                ContentValues cv = new ContentValues();
                cv.put(KEY_FIELD, data[i]);
                cv.put(VALUE_FIELD, data[i + 1]);
                insertData(cv);
            }
            Log.v("Me Log1 ", "Recovered Log data Total Count " + data.length / 2);
            //Releasing the lock aquired for lost messages insertions
            Lock lock = hmLock.get(myPort);
//            lock.unlock();
            init_Lock = true;
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
//                if (myPort.equals(ENTRY_NODE)) {
//                    //Getting all the nodes hashed and port in the circular
//                    //list and send to the node who just joined the system
//                    String node_list = node.getAllContent();
//                    String[] msgToSend= {NODE_UPDATE, newNodeId, node_list};
//                    sendMessageToClient(msgToSend);
//                }

                //This will stop the chain if next Node is entry node
                // as entry node only started the chain
//                if (!prevSuccessor.isEmpty() && !prevSuccessor.equals(ENTRY_NODE)) {
//                    String[] updateRing = {JOIN, newNodeId, prevSuccessor};
//                    sendMessageToClient(updateRing);
//                }
                Log.v("Me Log1", " Nodes Updated Ring List " + node.toString());
                // check and send message to relive node
                checkAndSendMessageRecoverNode(newNodeId);
            } catch (NoSuchAlgorithmException ex) {
                Log.e("Me Log Error", " Error in belong To Self SimpleDhtProvider " + ex.getMessage());
            }
        }

        private void checkAndSendMessageRecoverNode(String newNodeId) {
            String msg = "";
            if (node.CheckMySuccessors(newNodeId)) {
                MatrixCursor mc = getSelfData();
                mc.moveToFirst();
                int keyIndex = mc.getColumnIndex(KEY_FIELD);
                int valueIndex = mc.getColumnIndex(VALUE_FIELD);
                do {
                    if (mc.getCount() == 0) break;
                    String key = mc.getString(keyIndex);
                    if (belongToSelf(key)) {
                        msg += key + " " + mc.getString(valueIndex) + " ";
                    }
                } while (mc.moveToNext());
            } else if (node.CheckMyPredecessor(newNodeId)) {
                MatrixCursor mc = getSelfData();
                mc.moveToFirst();
                int keyIndex = mc.getColumnIndex(KEY_FIELD);
                int valueIndex = mc.getColumnIndex(VALUE_FIELD);
                do {
                    if (mc.getCount() == 0) break;
                    String key = mc.getString(keyIndex);
                    if (belongToPredecessor(key)) {
                        msg += key + " " + mc.getString(valueIndex) + " ";
                    }
                } while (mc.moveToNext());
            }
            if (!msg.isEmpty()) {
              String[] msgToSend = {LOST_MESSAGE, newNodeId, msg.trim()};
              sendMessageToClient(msgToSend);
            }
        }

        protected void onProgressUpdate(String... strings) {
            fireQueryAndReturnResults(strings[0]);
        }

        //msg format
        //msg[1] = KEY
        //msg[2] = originPort
        //msg[3] = uniqueID of the query
        private void fireQueryAndReturnResults(String msg) {
            Log.v("Me Log 1 ", msg + " QUERY Type");
            String keyString = msg.split(" ")[1];
            String uniqueId = msg.split(" ")[3];
            String[] originPorts = {msg.split(" ")[2], uniqueId};

            if (FULLDATA.equals(keyString)) {
                Cursor cr = getSelfData();//query(mUri, null, keyString, originPorts, null);
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
                } while (cr.moveToNext());
//                String[] msgToSend = {RESULT, node.getPredecessor(), uniqueId, keyValues.trim()};
                String[] msgToSend = {RESULT, originPorts[0], uniqueId, keyValues.trim()};
                sendMessageToClient(msgToSend);
            } else {
                //if we are fetching only one key value then return
                //directly to the requester
                String valueContent = getValueFromKey(keyString);
                String keyValues = "";
                if (!valueContent.isEmpty()) {
                    keyValues += keyString + " " + valueContent;
                }
                String[] msgToSend = {RESULT, originPorts[0], uniqueId, keyValues.trim()};
                sendMessageToClient(msgToSend);
            }
        }

        private void updateResultMapObject(String msg) {
            String[] data = msg.split(" ");
            String[] columns = {KEY_FIELD, VALUE_FIELD};
            String uniqueId = data[1];
            Log.v("Me Log ", "Putting the data for key " + uniqueId);
            if (hmResult.containsKey(uniqueId)) {
                MatrixCursor cr = hmCursor.get(uniqueId);
                for (int i = 2; i < data.length; i += 2) {
                    //to remove any space values
                    if (data[i].trim().isEmpty()) i++;
                    String[] row = new String[2];
                    row[0] = data[i];
                    row[1] = data[i + 1];
                    cr.addRow(row);
                }
                hmResult.put(uniqueId, hmResult.get(uniqueId) + 1);
            } else {
                MatrixCursor cr = new MatrixCursor(columns);
                for (int i = 2; i < data.length; i += 2) {
                    //to remove any space values
                    if (data[i].trim().isEmpty()) i++;
                    String[] row = new String[2];
                    row[0] = data[i];
                    row[1] = data[i + 1];
                    cr.addRow(row);
                }
                hmCursor.put(uniqueId, cr);
                hmResult.put(uniqueId, 1);
            }
//            hmResult.put(uniqueId, cr);
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... params) {
            String type = params[0];
            Log.wtf("Me Log ", "In Client Task " + Arrays.toString(params));
            if (NEWJOIN.equals(type) || JOIN.equals(type)) {
                String remotePort = null;
//                if (JOIN.equals(type)) {
//                    remotePort = Integer.parseInt(params[2]) * 2 + "";
//                }
//                else if (NEWJOIN.equals(type)) {
//                    remotePort = 5554 * 2 + "";
//                }
                for (int i = 0; i < REMOTE_PORTS.length; i++) {
                    try {
                        remotePort = REMOTE_PORTS[i];
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));
                        socket.setSoTimeout(300);
                        String msgToSend = JOIN + " " + params[1];
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        oos.writeObject(msgToSend);
                        oos.flush();
                        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                        String ack = (String)ois.readObject();
                        oos.flush();
                        oos.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                sendMessage(params);
            }
            return null;
        }

        private void sendMessage(String[] params) {
            String msgToSend = "";
            String remotePort = "";
            if (QUERY.equals(params[0])) {
                //param = "type" + " " remotePort + " "  + "key" + " " + "originPort" + " " + "unqieID"
                msgToSend = params[0] + " " + params[2] + " " + params[3] + " " + params[4];
                String myPortAddress = Integer.parseInt(myPort) * 2 + "";
                if (FULLDATA.equals(params[2])) {
                    for (int i = 0; i < REMOTE_PORTS.length; i++) {
                        if (!REMOTE_PORTS[i].equals(myPortAddress)) {
                            try {
                                remotePort = REMOTE_PORTS[i];
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(remotePort));
                                socket.setSoTimeout(300);
                                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                                oos.writeObject(msgToSend);
                                oos.flush();
                                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                                String ack = (String)ois.readObject();
//                                PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
//                                pw.println(msgToSend);
//                                pw.flush();
//                                pw.close();
                                ois.close();
                                oos.close();
                                socket.close();
                            } catch (UnknownHostException e) {
                                e.printStackTrace();
                            } catch (IOException e) {
                                Log.wtf("Me Log Got", e.getMessage());
                                e.printStackTrace();
                                hmResult.put(params[4], 1);
                                String[] columns = {KEY_FIELD, VALUE_FIELD};
                                MatrixCursor mc = new MatrixCursor(columns);
                                hmCursor.put(params[4], mc);
                            } catch (ClassNotFoundException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                } else { //if it is single key query then only query to other two/three avd's
                    String[] ports = new String[3];
                    ports[0] = Integer.parseInt(params[1]) * 2 + "";
                    ports[1] = Integer.parseInt(params[5]) * 2 + "";
                    ports[2] = Integer.parseInt(params[6]) * 2 + "";
                    for (int i = 0; i < ports.length; i++) {
                        try {
                            remotePort = ports[i];
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(remotePort));
                            socket.setSoTimeout(300);
//                            PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
//                            pw.println(msgToSend);
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            oos.writeObject(msgToSend);
                            oos.flush();
                            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                            String ack = (String)ois.readObject();
//                            pw.flush();
//                            pw.close();
                            oos.close();
                            socket.close();
                        } catch (UnknownHostException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    }
                }
            } else {
                if (NODE_UPDATE.equals(params[0])) {
                    //0. type, 1: remote Port, 2: all the hashedkeys and port values
                    remotePort = params[1];
                    msgToSend = params[0] + " " + params[2];
                } else if (MESSAGE_REPLICATE.equals(params[0]) ||
                        (MESSAGE.equals(params[0]))) {
                    //o. type, 1. remotePort, 2. key 3. value
                    remotePort = params[1];
                    msgToSend = params[0] + " " + params[2] + " " + params[3] + " " + params[4] + " " + params[5];
                } else if (RESULT.equals(params[0])) {
                    //params = "type" +" " + "remote_POrt" + " " + "uniqueID" + " " + "resultValue"
                    remotePort = params[1];
                    msgToSend = params[0] + " " + params[2] + " " + params[3];
                } else if (DELETE.equals(params[0])) {
                    remotePort = params[1];
                    msgToSend = params[0] + " " + params[2] + " " + params[3];
                } else if (LOST_MESSAGE.equals(params[0])) {
                    remotePort = params[1];
                    msgToSend = params[0] + " " + params[2];
                } else if (ACK_INSERT.equals(params[0])) {
                    remotePort = params[1];
                    msgToSend = params[0] + " " + params[2];
                } else {
                    Log.wtf("Me Log What", "!!!No type in Send Message" + params);
                }
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort) * 2);
                    socket.setSoTimeout(300);
//                    PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
//                    pw.println(msgToSend.trim());
//                    pw.flush();
//                    pw.close();
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(msgToSend);
                    oos.flush();
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    String ack = (String)ois.readObject();
                    socket.close();
                } catch (SocketTimeoutException ex) {
                    Log.e("Me Log Error:FAILED , ", ex.getMessage() + " ");
                } catch (UnknownHostException ex) {
                    Log.e("Me Log Error: FAILED , ", ex.getMessage() + " ");
                } catch (IOException ex) {
                    Log.e("Me Log Error: FAILED , ", ex.getMessage() + " ");
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }

            }
        }
    }
}
