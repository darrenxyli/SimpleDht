package edu.buffalo.cse.cse486586.simpledht;

import android.net.Uri;
import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

/**
 * SimpleDht
 * <p/>
 * Created by darrenxyli on 3/26/15.
 * Changed by darrenxyli on 3/26/15 6:25 PM.
 */
public class Until {

    private static String TAG = Until.class.getSimpleName();

    public static final int SERVER_PORT = 10000;
    public static final int COORD_SERVER = 11108;
    public static final String DATABASE_AUTHORITY = "edu.buffalo.cse.cse486586.simpledht.provider";
    public static final String DATABASE_SCHEME = "content";
    public static final Uri DATABASE_CONTENT_URL = buildUri(DATABASE_SCHEME, DATABASE_AUTHORITY);

    /**
     * buildUri() demonstrates how to build a URI for a ContentProvider.
     *
     * @param scheme String
     * @param authority String
     * @return the URI
     */
    public static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


    public static String genHash(String input) {
        MessageDigest sha1 = null;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, e.toString());
        }
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
