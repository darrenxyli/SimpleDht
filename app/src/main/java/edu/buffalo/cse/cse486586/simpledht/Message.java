package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.HashMap;

/**
 * SimpleDht
 * <p/>
 * Created by darrenxyli on 3/26/15.
 * Changed by darrenxyli on 3/26/15 6:14 PM.
 */
public class Message implements Serializable {

    private static final long serialVersionUID = 5393191541861509332L;

    public static enum type {
        INSERT, DELETE, QUERY,
        INSERT_ACK, DELETE_ACK, QUERY_ACK,
        JOIN, STABILIZATION
    };

    public type msgType;
    public int originPort; // who sends this
    public int predecessorPort; // store predecessor port
    public int successorPort; // store successor port
    public int forwardPort; // who will be sented next
    public String key; // key to store
    public String value; // value to store

    public HashMap<String, String> batch = new HashMap<>();
}
