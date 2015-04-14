package edu.buffalo.cse.cse486586.simpledht;

import android.provider.BaseColumns;

/**
 * SimpleDht
 * <p/>
 * Created by darrenxyli on 3/26/15.
 * Changed by darrenxyli on 3/26/15 6:48 PM.
 */
public final class DbSchema {
    // To prevent someone from accidentally instantiating the contract class,
    // give it an empty constructor.
    public DbSchema() {}

    /* Inner class that defines the table contents */
    public static abstract class DbEntry implements BaseColumns {
        public static final String TABLE_NAME = "messages";
        public static final String COLUMN_NAME_KEY = "key";
        public static final String COLUMN_NAME_VALUE = "value";
        public static final String COLUMN_NAME_HASHKEY = "hashkey";
    }
}
