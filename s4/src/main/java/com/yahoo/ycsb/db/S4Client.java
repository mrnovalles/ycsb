/**
 * S4 client binder for YCSB - 1st try
 * 
 * */

package com.yahoo.ycsb.db;

import org.apache.s4.client.Driver;
import org.apache.s4.client.Message;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

public class S4Client extends com.yahoo.ycsb.DB {
    
    public static final String HOST_PROPERTY = "s4.host";
    public static final String PORT_PROPERTY = "s4.port";
    private Driver d=null ;
    
    public void init() throws DBException{
        Properties props = getProperties();
        int port;
        
        String portString = props.getProperty(PORT_PROPERTY);
        if (portString != null) {
            port = Integer.parseInt(portString);
        }
        //FIXME: Ugly hard-coded port
        else{
            port =Integer.parseInt("2334");
        }
        String host = props.getProperty(HOST_PROPERTY);
        System.err.println("About to connect to"+ host + port);
        d = new Driver(host, port);
        try {
            if (!d.init()) {
                System.err.println("Driver initialization failed");
                System.exit(1);
            }

            if (!d.connect()) {
                System.err.println("Driver initialization failed");
                System.exit(1);           
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }
    public void cleanup(){
        try { d.disconnect(); } catch (Exception e) {}
    }

    @Override
    public int delete(String arg0, String arg1) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    /**
     * TODO: A naive insert for now will only use the key parameter to send
     * to the S4 cluster and has "Word" harcoded and will need a word application
     */
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        String string = "{\"string\":\""+table+" "+table+"."+"\"}";

	
	/* TODO: RawWords should be a command line parameter, may be table name
	  Closely related to the deployed application.	*/
        Message m = new Message("RawWords","test.s4.Word", string);
        try {
            d.send(m);
            return 0;
        } catch (IOException e) {
            e.printStackTrace();
            return 1;
        }
        
    }

    @Override
    //  public int read(String table, String key, Set<String> fields, HashMap<String,String> result);
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int scan(String arg0, String arg1, int arg2, Set<String> arg3, Vector<HashMap<String, ByteIterator>> arg4) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int update(String arg0, String arg1, HashMap<String, ByteIterator> arg2) {
        // TODO Auto-generated method stub
        return 0;
    }
}
