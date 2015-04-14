package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    private static String TAG = SimpleDhtProvider.class.getSimpleName();


    Context context; // application context
    DbHelper sDbHelper; // database helper
    SQLiteDatabase db; // database object
    private Object dbLock = new Object();
    public static Cursor curs; // cursor object used to store the cursor returned by query


    public static int predecessorPort;
    public static int successorPort;
    public static int localPort;


    public static String predecessorId; // hash of port
    public static String successorId;
    public static String localId;



    /**
     * Calculate the port number that this AVD listens on.
     */
    private int getLocalPort() {
        TelephonyManager tel =
                (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        return (Integer.parseInt(portStr) * 2);
    }


    private boolean checkLocal(String key) {

        //if (localId.equals(predecessorId) && localId.equals(successorId)) return true;

        if (localId.compareTo(predecessorId) < 0){

            return (key.compareTo(localId) <= 0 || key.compareTo(predecessorId) > 0);

        } else if (localId.compareTo(predecessorId) == 0) {

            return true;

        } else {

            return (key.compareTo(predecessorId) > 0 && key.compareTo(localId) <= 0);

        }
    }


    @Override
    public int delete(Uri uri, String oselection, String[] selectionArgs) {

        int rows = 0;
        String selection = (oselection != null && oselection.contains("\"")) ? oselection.substring(1, oselection.length()-1) : oselection;
        String key = (selection != null)? Until.genHash(selection) : "";

        // Situation 1:
        // Selection be @, then delete all on this device;
        //
        // Situation 2:
        // Selection be *, means delete all on all device. Then delete on local firstly then notify
        // every others to delete it on LOCAL.
        //
        // Situation 3:
        // Selection be particular one, then delete that



        if (selection.equals("@")) {

            Log.d(TAG, "DELETE @");

            synchronized (dbLock) {
                rows = db.delete(DbSchema.DbEntry.TABLE_NAME, null, null);
            }

            Log.d(TAG, "DELETE @ finished");

            return rows;

        } else if (selection.equals("*")) {

            Log.d(TAG, "DELETE *");


            synchronized (dbLock) {
                rows = db.delete(DbSchema.DbEntry.TABLE_NAME, null, null);
            }

            int nextPort = successorPort;

            while (nextPort != localPort) {


                Message findDelMsg = new Message();
                findDelMsg.msgType = Message.type.DELETE;
                findDelMsg.key = "\"@\"";
                findDelMsg.forwardPort = nextPort;

                try {

                    // Create socket
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            findDelMsg.forwardPort);

                    // Create out stream
                    ObjectOutputStream out = new ObjectOutputStream(
                            new BufferedOutputStream(socket.getOutputStream()));

                    // Write message
                    out.writeObject(findDelMsg);
                    out.flush();

                    Log.d(TAG, "Send DELETE @ to " + nextPort);

                    // Wait back
                    ObjectInputStream in = new ObjectInputStream(
                            new BufferedInputStream(socket.getInputStream()));

                    Message m = (Message) in.readObject();

                    if (m.msgType == Message.type.DELETE_ACK) {

                        Log.d(TAG, "Receive DELETE_ACK from " + nextPort + ", update next to " + m.successorPort);

                        rows += m.predecessorPort; // predecessorPort saves the deleted rows on remote
                        nextPort = m.successorPort;

                    }

                    out.close();
                    in.close();
                    socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, e.toString());
                } catch (IOException e) {
                    Log.e(TAG, e.toString());
                } catch (ClassNotFoundException e) {
                    Log.e(TAG, e.toString());
                }
            }

            Log.d(TAG, "DELETE * finished");

            return rows;


        } else {

            Log.d(TAG, "DELETE " + selection);


//            if ((localId.equals(predecessorId) && localId.equals(successorId))
//                    || (key.compareTo(predecessorId) > 0 && key.compareTo(localId) <= 0)
//                    || (localId.compareTo(predecessorId) < 0 && (key.compareTo(localId) <= 0
//                    || key.compareTo(predecessorId) > 0))) {

            if (checkLocal(key)) {

                Log.d(TAG, "DELETE " + selection + " in " + localPort);


                synchronized (dbLock) {
                    rows = db.delete(DbSchema.DbEntry.TABLE_NAME,
                            DbSchema.DbEntry.COLUMN_NAME_HASHKEY + "= ?", new String[] { key });
                }

                Log.d(TAG, "DELETE " + selection + " finished");

                return rows;

            } else {

                int nextPort = successorPort;
                boolean delFinish = false;

                while (!delFinish) {


                    Message findInsertMsg = new Message();
                    findInsertMsg.msgType = Message.type.DELETE;
                    findInsertMsg.key = selection;
                    findInsertMsg.forwardPort = nextPort;

                    try {

                        // Create socket
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                findInsertMsg.forwardPort);

                        // Create out stream
                        ObjectOutputStream out = new ObjectOutputStream(
                                new BufferedOutputStream(socket.getOutputStream()));

                        // Write message
                        out.writeObject(findInsertMsg);
                        out.flush();

                        Log.d(TAG, "Resend DELETE " + selection + " to " + nextPort);

                        // Wait back
                        ObjectInputStream in = new ObjectInputStream(
                                new BufferedInputStream(socket.getInputStream()));

                        Message m = (Message) in.readObject();

                        if (m.msgType == Message.type.DELETE_ACK) {

                            Log.d(TAG, "DELETE " + selection + " in " + nextPort);

                            delFinish = true;

                            out.close();
                            in.close();
                            socket.close();

                            return m.predecessorPort; // predecessorPort save the deleted rows.

                        } else {

                            Log.d(TAG, "DELETE " + selection + " not in " + nextPort);

                            nextPort = m.successorPort;

                        }

                        out.close();
                        in.close();
                        socket.close();

                    } catch (UnknownHostException e) {
                        Log.e(TAG, e.toString());
                    } catch (IOException e) {
                        Log.e(TAG, e.toString());
                    } catch (ClassNotFoundException e) {
                        Log.e(TAG, e.toString());
                    }
                }

            }

        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String selection = values.getAsString(DbSchema.DbEntry.COLUMN_NAME_KEY);

        String key = (selection != null)? Until.genHash(selection) : "";
        String value = values.getAsString(DbSchema.DbEntry.COLUMN_NAME_VALUE);


        // If insert value belong to this node, insert
        // Else Optional way: pass insert information to next one ?
        // Current way: round query each node.

//        if ((localId.equals(predecessorId) && localId.equals(successorId))
//                || (key.compareTo(predecessorId) > 0 && key.compareTo(localId) <= 0)
//                || (localId.compareTo(predecessorId) < 0 && (key.compareTo(localId) <= 0
//                || key.compareTo(predecessorId) > 0))) {

        if (checkLocal(key)) {

            try {

                ContentValues cv = new ContentValues();
                cv.put(DbSchema.DbEntry.COLUMN_NAME_KEY, selection);
                cv.put(DbSchema.DbEntry.COLUMN_NAME_HASHKEY, key);
                cv.put(DbSchema.DbEntry.COLUMN_NAME_VALUE, value);


                long newRowId;
                synchronized (dbLock) {
                    newRowId = db.insertOrThrow(DbSchema.DbEntry.TABLE_NAME, null, cv);
                }
                Log.d(TAG, "Insert:("+newRowId+") HASHKEY: " + key + " KEY: " + selection + " VALUE: " + value);

                return Uri.withAppendedPath(Until.DATABASE_CONTENT_URL, String.valueOf(newRowId));

            } catch (SQLiteException e) {
                Log.e(TAG, e.toString());
            }

        } else {

            int nextPort = successorPort;
            boolean insertFinish = false;

            while (!insertFinish) {

                Message findInsertMsg = new Message();
                findInsertMsg.msgType = Message.type.INSERT;
                findInsertMsg.key = selection;
                findInsertMsg.value = value;
                findInsertMsg.originPort = localPort;
                findInsertMsg.forwardPort = nextPort;

                try {

                    // Create socket
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            findInsertMsg.forwardPort);

                    // Create out stream
                    ObjectOutputStream out = new ObjectOutputStream(
                            new BufferedOutputStream(socket.getOutputStream()));

                    // Write message
                    out.writeObject(findInsertMsg);
                    out.flush();

                    Log.d(TAG, "Resend INSERT to " + nextPort  + ": HASHKEY: " + key + " KEY: " + selection + " VALUE: " + value);

                    // Wait back
                    ObjectInputStream in = new ObjectInputStream(
                            new BufferedInputStream(socket.getInputStream()));

                    Message m = (Message) in.readObject();

                    if (m.msgType == Message.type.INSERT_ACK) {

                        Log.d(TAG, "INSERT " + selection + " finished in " + nextPort);

                        insertFinish = true;

                    } else {

                        Log.d(TAG, "INSERT " + selection + " not in " + nextPort);

                        nextPort = m.successorPort;

                    }

                    out.close();
                    in.close();
                    socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, e.toString());
                } catch (IOException e) {
                    Log.e(TAG, e.toString());
                } catch (ClassNotFoundException e) {
                    Log.e(TAG, e.toString());
                }
            }

        }
//        else {
//
//            Message notifyNext = new Message();
//            notifyNext.msgType = Message.type.INSERT;
//            notifyNext.key = key;
//            notifyNext.value = value;
//            notifyNext.originPort = localPort;
//            notifyNext.forwardPort = successorPort;
//
//            Send(notifyNext);
//        }
        return null;
    }

    @Override
    public boolean onCreate() {

        context = getContext();
        sDbHelper = new DbHelper(context);
        db = sDbHelper.getWritableDatabase();


        localPort = getLocalPort();
        localId = Until.genHash(String.valueOf(localPort/2));
        predecessorPort = successorPort = localPort;
        predecessorId = successorId = localId;

        Log.d(TAG, "My PORT: " + localPort + ", my HASH: " + localId);


        // Start thread listening on port
        new Thread(new Server(context)).start();


         // When this device is not central server, notify the server it is ready to join the chord
         // ring.
        if (localPort != Until.COORD_SERVER) {
            try {

                Message m = new Message();
                m.msgType = Message.type.JOIN;
                m.forwardPort = Until.COORD_SERVER;
                m.originPort = localPort;
                new Thread(new Client(m)).start();

                Log.d(TAG, String.valueOf(localPort/2) + " sent JOIN to " + Until.COORD_SERVER);

            } catch (Exception e) {
                Log.e(TAG, e.toString());
                e.printStackTrace();
            }
        }


        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String oselection, String[] selectionArgs,
            String sortOrder) {

        Cursor cursor;
        String selection = (oselection != null && oselection.contains("\"")) ? oselection.substring(1, oselection.length()-1) : oselection;
        String key = (selection != null)? Until.genHash(selection) : "";

        Log.d(TAG, "QUERY: KEY: " + selection + " HASHKEY: " + key);

        // Addition Situation: batch query
        if (selectionArgs != null) {

            Log.d(TAG, "BATCH QUERY from id " + selectionArgs[0]);

            synchronized (dbLock) {
                cursor = db.rawQuery("SELECT * from " + DbSchema.DbEntry.TABLE_NAME
                        + " WHERE " + DbSchema.DbEntry.COLUMN_NAME_HASHKEY + " <= ?", new String[]{ selectionArgs[0] });
            }
            cursor.setNotificationUri(context.getContentResolver(), uri);

            return cursor;

        }


        // Similarly, there are 3 situation in query:
        //
        // Situation 1:
        // Selection be @, query all items locally, return Cursor.
        //
        // Situation 2:
        // Selection be *, query all items globally
        //
        // Situation 3:
        // Selection be particular, round query.

        if (selection.equals("@")) {

            Log.d(TAG, "QUERY @");
            synchronized (dbLock) {
                cursor = db.rawQuery("SELECT * from " + DbSchema.DbEntry.TABLE_NAME, selectionArgs);
            }
            cursor.setNotificationUri(context.getContentResolver(), uri);

            Log.d(TAG, "QUERY @ finished");

            return cursor;

        } else if (selection.equals("*")) {

            Log.d(TAG, "QUERY *");

            synchronized (dbLock) {
                cursor = db.rawQuery("SELECT * from " + DbSchema.DbEntry.TABLE_NAME, selectionArgs);
            }
            cursor.setNotificationUri(context.getContentResolver(), uri);

            curs = new MatrixCursor(new String[] {
                    DbSchema.DbEntry.COLUMN_NAME_KEY, DbSchema.DbEntry.COLUMN_NAME_VALUE});

            if (cursor != null) {
                int keyIndex = cursor.getColumnIndex(DbSchema.DbEntry.COLUMN_NAME_KEY);
                int valueIndex = cursor.getColumnIndex(DbSchema.DbEntry.COLUMN_NAME_VALUE);
                if (keyIndex != -1 && valueIndex != -1) {
                    if (cursor.moveToFirst()) {
                        do {

                            ((MatrixCursor) curs).addRow(new String[]{
                                    cursor.getString(keyIndex), cursor.getString(valueIndex)});

                        } while (cursor.moveToNext());
                    }
                }
                cursor.close();
            }

            int nextPort = successorPort;

            while (nextPort != localPort) {



                Message findQueryMsg = new Message();
                findQueryMsg.msgType = Message.type.QUERY;
                findQueryMsg.key = "\"@\"";
                findQueryMsg.forwardPort = nextPort;

                try {

                    // Create socket
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            findQueryMsg.forwardPort);

                    // Create out stream
                    ObjectOutputStream out = new ObjectOutputStream(
                            new BufferedOutputStream(socket.getOutputStream()));

                    // Write message
                    out.writeObject(findQueryMsg);
                    out.flush();

                    Log.d(TAG, "Send QUERY @ to " + nextPort);


                    // Wait back
                    ObjectInputStream in = new ObjectInputStream(
                            new BufferedInputStream(socket.getInputStream()));

                    Message m = (Message) in.readObject();

                    if (m.msgType == Message.type.QUERY_ACK) {

                        Log.d(TAG, "Receive QUERY_ACK from " + nextPort + ", update next to " + m.successorPort);

                        for (String k : m.batch.keySet()) {
                            ((MatrixCursor) curs).addRow(new String[] {
                                    k, m.batch.get(k) });
                        }

                        nextPort = m.successorPort;
                    }

                    out.close();
                    in.close();
                    socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, e.toString());
                } catch (IOException e) {
                    Log.e(TAG, e.toString());
                } catch (ClassNotFoundException e) {
                    Log.e(TAG, e.toString());
                }
            }

            return curs;


        } else {

            Log.d(TAG, "QUERY " + selection);


//            if ((localId.equals(predecessorId) && localId.equals(successorId))
//                    || (key.compareTo(predecessorId) > 0 && key.compareTo(localId) <= 0)
//                    || (localId.compareTo(predecessorId) < 0 && (key.compareTo(localId) <= 0
//                    || key.compareTo(predecessorId) > 0))) {

            if (checkLocal(key)) {

                Log.d(TAG, "QUERY " + selection + " in " + localPort);

                synchronized (dbLock) {
                    cursor = db.rawQuery("SELECT * from " + DbSchema.DbEntry.TABLE_NAME
                            + " WHERE " + DbSchema.DbEntry.COLUMN_NAME_HASHKEY + " = ?", new String[]{ key });
                }

                cursor.setNotificationUri(context.getContentResolver(), uri);

                Log.d(TAG, "QUERY " + selection + " finished");

                return cursor;

            } else {

                int nextPort = successorPort;
                boolean queryFinish = false;

                curs = new MatrixCursor(new String[] {
                        DbSchema.DbEntry.COLUMN_NAME_KEY, DbSchema.DbEntry.COLUMN_NAME_VALUE});

                while (!queryFinish) {

                    Message findQueryMsg = new Message();
                    findQueryMsg.msgType = Message.type.QUERY;
                    findQueryMsg.key = oselection;
                    findQueryMsg.originPort = localPort;
                    findQueryMsg.forwardPort = nextPort;

                    try {

                        // Create socket
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                findQueryMsg.forwardPort);

                        // Create out stream
                        ObjectOutputStream out = new ObjectOutputStream(
                                new BufferedOutputStream(socket.getOutputStream()));

                        // Write message
                        out.writeObject(findQueryMsg);
                        out.flush();

                        Log.d(TAG, "Resend QUERY " + selection + " to " + nextPort);

                        // Wait back
                        ObjectInputStream in = new ObjectInputStream(
                                new BufferedInputStream(socket.getInputStream()));

                        Message m = (Message) in.readObject();

                        if (m.msgType == Message.type.QUERY_ACK) {

                            Log.d(TAG, "QUERY " + selection + " finished in " + nextPort);

                            queryFinish = true;

                            for (String k : m.batch.keySet()) {
                                ((MatrixCursor) curs).addRow(new String[] {
                                        k, m.batch.get(k) });
                            }

                            // ADD in 30 13:00
                            out.close();
                            in.close();
                            socket.close();
                            return curs;

                        } else {

                            Log.d(TAG, "QUERY " + selection + " not in " + nextPort);

                            nextPort = m.successorPort;

                        }

                        out.close();
                        in.close();
                        socket.close();

                    } catch (UnknownHostException e) {
                        Log.e(TAG, e.toString());
                    } catch (IOException e) {
                        Log.e(TAG, e.toString());
                    } catch (ClassNotFoundException e) {
                        Log.e(TAG, e.toString());
                    }
                }

            }

        }


        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }


    /**
     *
     *  Server
     *
     *
     *
     */

    public class Server implements Runnable {

        private String TAG = Server.class.getSimpleName();

        Context context;

        public Server(Context c) {
            this.context = c;
        }

        @Override
        public void run() {

            // Create server socket
            ServerSocket serverSocket = null;
            Socket clientSocket = null;
            try {
                serverSocket = new ServerSocket(Until.SERVER_PORT);
            } catch (IOException e) {
                Log.e(TAG, e.toString());
            }


            while (true) {
                try {

                    clientSocket = serverSocket.accept();
                    ObjectInputStream input = new ObjectInputStream(
                            new BufferedInputStream(clientSocket.getInputStream()));


                    Message msg = (Message) input.readObject();

                    Log.d(TAG, "Received Message: " + msg.msgType + ", From: " + msg.originPort);

                    int origPort = msg.originPort;

                    String selection;
                    String key;
                    Message reply;

                    switch (msg.msgType) {


                        // For JOIN
                        // - Situation 1:
                        //     If there is only server node
                        //     Or the JOIN-node locates between current node and successor
                        //     Or the JOIN-node locates between server node and last node
                        //     Then notify JOIN-node change its pre to current and suc to successor
                        //     After that, notify the successor to change its pre to JOIN-node
                        //     At last, change successor of current to JOIN-node.
                        //
                        // - Situation 2:
                        //     If JOIN-node locates after successor, then send message to successor.
                        //

                        case JOIN:

                            input.close();
                            clientSocket.close();

                            String origId = Until.genHash(String.valueOf(origPort/2));

                            if ((localId.equals(predecessorId) && localId.equals(successorId))
                                    || (origId.compareTo(localId) > 0 && origId.compareTo(successorId) < 0)
                                    || (localId.compareTo(successorId) > 0 && (origId.compareTo(localId) > 0
                                    || origId.compareTo(successorId) < 0))) {


                                Message notifyJoin = new Message();
                                notifyJoin.msgType = Message.type.STABILIZATION;
                                notifyJoin.predecessorPort = localPort;
                                notifyJoin.successorPort = successorPort;
                                notifyJoin.originPort = localPort;
                                notifyJoin.forwardPort = origPort;
                                Send(notifyJoin);


                                Message notifySuc = new Message();
                                notifySuc.msgType = Message.type.STABILIZATION;
                                notifySuc.predecessorPort = origPort;
                                notifySuc.successorPort = -1;
                                notifySuc.originPort = localPort;
                                notifySuc.forwardPort = successorPort;
                                Send(notifySuc);


                                successorPort = origPort;
                                successorId = origId;

                                Log.d(TAG, "Send STABILIZATION finished");

                            } else {

                                Message notifyNext = new Message();
                                notifyNext.msgType = Message.type.JOIN;
                                notifyNext.originPort = origPort;
                                notifyNext.forwardPort = successorPort;
                                Send(notifyNext);

                                Log.d(TAG, "Resend JOIN to " + successorPort + "finished");

                            }

                            msg = null;

                            break;


                        // When receive INSERT, there are 2 situation:
                        //
                        // Situation 1: Single insert
                        // In this situation, batch field will be empty. But key, value would not.
                        // So we insert it. If key locates in there directly insert it and reply
                        // with ACK, otherwise reply to server with successor.
                        //
                        // Situation 2: Batch insert
                        // In this situation, batch field in Message would be not empty, so for each
                        // element in that field, we insert all one by one using single insert.


                        case INSERT:

                            if (msg.batch.isEmpty()) {

                                selection = msg.key;

                                key = Until.genHash(selection);
                                String val = msg.value;

                                reply = new Message();


                                // if ((localId.equals(predecessorId) && localId.equals(successorId))
                                //        || (key.compareTo(predecessorId) > 0 && key.compareTo(localId) <= 0)
                                //        || (localId.compareTo(predecessorId) < 0 && (key.compareTo(localId) <= 0
                                //        || key.compareTo(predecessorId) > 0))) {

                                if (checkLocal(key)) {
                                    ContentValues cv = new ContentValues();
                                    cv.put(DbSchema.DbEntry.COLUMN_NAME_KEY, selection);
                                    cv.put(DbSchema.DbEntry.COLUMN_NAME_VALUE, val);

                                    Uri u = insert(Until.DATABASE_CONTENT_URL, cv);

                                    reply.msgType = Message.type.INSERT_ACK;

                                    Log.d(TAG, "INSERT " + selection + " in " + localPort + ", send back ACK");


                                } else {

                                    reply.msgType = Message.type.INSERT;
                                    reply.successorPort = successorPort;

                                    Log.d(TAG, "INSERT " + selection + " not in " + localPort + ", send back next");

                                }


                                // Create out stream
                                ObjectOutputStream out = new ObjectOutputStream(
                                        new BufferedOutputStream(clientSocket.getOutputStream()));

                                // Write message
                                out.writeObject(reply);
                                out.flush();

                                Log.d(TAG, "Reply sent");

                            } else {

                                Iterator iter = msg.batch.entrySet().iterator();
                                while (iter.hasNext()) {
                                    Map.Entry entry = (Map.Entry) iter.next();
                                    key = entry.getKey().toString();
                                    String val = entry.getValue().toString();

                                    ContentValues cv = new ContentValues();
                                    cv.put(DbSchema.DbEntry.COLUMN_NAME_KEY, key);
                                    cv.put(DbSchema.DbEntry.COLUMN_NAME_VALUE, val);

                                    insert(Until.DATABASE_CONTENT_URL, cv);
                                }
                            }

                            msg = null;

                            break;

                        case DELETE:


                            // For Delete
                            // Situation 1:
                            // When received key == "@", then delete locally, reply the ACK with
                            // deleted rows and successor;
                            //
                            // Situation 2:
                            // When received key == particular, if key in there, then delete and
                            // and return the rows, otherwise return the successor.


                            selection = msg.key;
                            key = Until.genHash(selection);
                            reply = new Message();

                            // Create out stream
                            ObjectOutputStream out = new ObjectOutputStream(
                                    new BufferedOutputStream(clientSocket.getOutputStream()));


                            if (selection.equals("@")) {

                                Log.d(TAG, "Received DELETE @");

                                int rows = delete(Until.DATABASE_CONTENT_URL, selection, null);
                                reply.msgType = Message.type.DELETE_ACK;
                                reply.predecessorPort = rows; // predecessor to save delete rows.
                                reply.successorPort = successorPort; // return the successor

                                Log.d(TAG, "Received DELETE @ finished, send back ACK with next");

                            } else {

                                Log.d(TAG, "Received DELETE " + selection);

                                if ((localId.equals(predecessorId) && localId.equals(successorId))
                                        || (key.compareTo(predecessorId) > 0 && key.compareTo(localId) <= 0)
                                        || (localId.compareTo(predecessorId) < 0 && (key.compareTo(localId) <= 0
                                        || key.compareTo(predecessorId) > 0))) {

                                    // Delete this
                                    int rows = delete(Until.DATABASE_CONTENT_URL, selection, null);
                                    reply.msgType = Message.type.DELETE_ACK;
                                    reply.predecessorPort = rows;

                                    Log.d(TAG, "DELETE " + selection + " finished, send back ACK");

                                } else {

                                    Log.d(TAG, "DELETE " + selection + " not in " + localPort + ", send back");

                                    reply.msgType = Message.type.DELETE;
                                    reply.successorPort = successorPort;

                                }

                            }

                            // Write message
                            out.writeObject(reply);
                            out.flush();

                            Log.d(TAG, "Reply finished");

                            msg = null;

                            break;

                        case QUERY:


                            selection = msg.key.contains("\"") ? msg.key.substring(1, msg.key.length()-1) : msg.key;
                            key = Until.genHash(selection);
                            reply = new Message();

                            // Create out stream
                            ObjectOutputStream outQ = new ObjectOutputStream(
                                    new BufferedOutputStream(clientSocket.getOutputStream()));


                            if (selection.equals("@")) {

                                Log.d(TAG, "Received QUERY @");

                                Cursor cur = query(Until.DATABASE_CONTENT_URL, null, selection, null, null);
                                reply.msgType = Message.type.QUERY_ACK;

                                if (cur != null) {
                                    int keyIndex = cur.getColumnIndex(DbSchema.DbEntry.COLUMN_NAME_KEY);
                                    int valueIndex = cur.getColumnIndex(DbSchema.DbEntry.COLUMN_NAME_VALUE);
                                    if (keyIndex != -1 && valueIndex != -1) {
                                        if (cur.moveToFirst()) {
                                            do {
                                                reply.batch.put(cur.getString(keyIndex),
                                                        cur.getString(valueIndex));
                                            } while (cur.moveToNext());
                                        }
                                    }
                                    cur.close();
                                }

                                reply.successorPort = successorPort; // return the successor

                                Log.d(TAG, "Received QUERY @ finished, send back ACK with next");

                            } else {

                                Log.d(TAG, "Received QUERY " + selection);

                                if ((localId.equals(predecessorId) && localId.equals(successorId))
                                        || (key.compareTo(predecessorId) > 0 && key.compareTo(localId) <= 0)
                                        || (localId.compareTo(predecessorId) < 0 && (key.compareTo(localId) <= 0
                                        || key.compareTo(predecessorId) > 0))) {

                                    // Query this
                                    Cursor cur = query(Until.DATABASE_CONTENT_URL, null, selection, null, null);
                                    reply.msgType = Message.type.QUERY_ACK;

                                    if (cur != null) {
                                        int keyIndex = cur.getColumnIndex(DbSchema.DbEntry.COLUMN_NAME_KEY);
                                        int valueIndex = cur.getColumnIndex(DbSchema.DbEntry.COLUMN_NAME_VALUE);
                                        if (keyIndex != -1 && valueIndex != -1) {
                                            if (cur.moveToFirst()) {
                                                do {
                                                    reply.batch.put(cur.getString(keyIndex),
                                                            cur.getString(valueIndex));
                                                } while (cur.moveToNext());
                                            }
                                        }
                                        cur.close();
                                    }

                                    Log.d(TAG, "QUERY " + selection + " finished, send back ACK");

                                } else {

                                    Log.d(TAG, "QUERY " + selection + " not in " + localPort + ", send back");

                                    reply.msgType = Message.type.QUERY;
                                    reply.successorPort = successorPort;

                                }

                            }

                            // Write message
                            outQ.writeObject(reply);
                            outQ.flush();

                            Log.d(TAG, "Reply finished");

                            msg = null;

                            break;

                        case STABILIZATION:

                            input.close();
                            clientSocket.close();

                            Log.d(TAG, "--------- Begin STABILIZATION ---------");

                            // When pre of node is changed, then current node has to move data which
                            // belongs to that node.
                            //
                            // 1. Need batch query from local database that key <= predecessorId
                            // 2. Send request to predecessor to let him insert these data.

                            if (msg.predecessorPort != -1) {

                                predecessorPort = msg.predecessorPort;
                                predecessorId = Until.genHash(String.valueOf(predecessorPort/2));

                                Log.d(TAG, localPort + "'s predecessor updated to " + predecessorPort + " with HASH: " + predecessorId);

                                // TODO: Batch query data from local DB
                                //
                                // Cursor cus = query(Until.DATABASE_CONTENT_URL, null, "\""+String.valueOf(predecessorPort/2)+"\"", new String[] { predecessorId }, null);


                                // TODO: send insert request to predecessor
                                //
//                                Message moveMsg = new Message();
//                                moveMsg.msgType = Message.type.INSERT;
//
//                                if (cus != null) {
//                                    int okeyIndex = cus.getColumnIndex(DbSchema.DbEntry.COLUMN_NAME_HASHKEY);
//                                    int keyIndex = cus.getColumnIndex(DbSchema.DbEntry.COLUMN_NAME_KEY);
//                                    int valueIndex = cus.getColumnIndex(DbSchema.DbEntry.COLUMN_NAME_VALUE);
//                                    if (keyIndex != -1 && valueIndex != -1) {
//                                        if (cus.moveToFirst()) {
//                                            do {
//                                                moveMsg.batch.put(cus.getString(keyIndex),
//                                                        cus.getString(valueIndex));
//                                            } while (cus.moveToNext());
//
//                                            // Create socket
//                                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), predecessorPort);
//
//                                            // Create out stream
//                                            ObjectOutputStream o = new ObjectOutputStream(
//                                                    new BufferedOutputStream(socket.getOutputStream()));
//
//                                            // Write message
//                                            o.writeObject(msg);
//                                            o.flush();
//
//                                            Log.d(TAG, "From STABILIZATION part send out.");
//                                            cus.close();
//                                        }
//                                    }
//                                }
                            }

                            if (msg.successorPort != -1) {

                                successorPort = msg.successorPort;
                                successorId = Until.genHash(String.valueOf(successorPort/2));

                                Log.d(TAG, localPort + "'s successor updated to " + successorPort + " with HASH: " + successorId);

                            }

                            Log.d(TAG, "--------- End STABILIZATION ---------");

                            msg = null;

                            break;

                        default:

                            msg = null;

                            input.close();
                            clientSocket.close();

                            break;

                    }



                } catch (ClassNotFoundException e) {
                    Log.e(TAG, e.toString());
                } catch (IOException e) {
                    Log.e(TAG, e.toString());
                }

            }
        }
    }

    public class Client implements Runnable {

        private String TAG = Client.class.getSimpleName();

        Message msg;

        public Client(Message m) {
            this.msg = m;
        }

        @Override
        public void run() {
            try {

                // Create socket
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), msg.forwardPort);

                // Create out stream
                ObjectOutputStream out = new ObjectOutputStream(
                        new BufferedOutputStream(socket.getOutputStream()));

                // Write message
                out.writeObject(msg);
                out.flush();

                Log.d(TAG, "Send from Client Thread");

                // Try not kill the socket in here
                // Close socket
                // socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, e.toString());
            } catch (IOException e) {
                Log.e(TAG, e.toString());
            }
        }
    }


    public void Send(Message msg) {

        try {


            // Create socket
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), msg.forwardPort);

            // Create out stream
            ObjectOutputStream out = new ObjectOutputStream(
                    new BufferedOutputStream(socket.getOutputStream()));

            // Write message
            out.writeObject(msg);
            out.flush();


            Log.d(TAG, "Send " + msg.msgType + " to " + msg.forwardPort + ", from Send functions");


            // Close socket
            socket.close();

        } catch (UnknownHostException e) {
            Log.e(TAG, e.toString());
            e.printStackTrace();
        } catch (IOException e) {
            Log.e(TAG, e.toString());
            e.printStackTrace();
        }
    }

}
