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
    private Driver d=null;
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
        finally {
            try { d.disconnect(); } catch (Exception e) {}
        }
        
    }
   /* public static void main(String[] args) {
		if (args.length < 1) {
            System.err.println("No host name specified");
            System.exit(1);
        }
        String hostName = args[0];
        
        if (args.length < 2) {
            System.err.println("No port specified");
            System.exit(1);
        }
        
        int port = -1;
        try {
            port = Integer.parseInt(args[1]);
        } catch (NumberFormatException nfe) {
            System.err.println("Bad port number specified: " + args[1]);
            System.exit(1);
        }
        
        if (args.length < 3) {
            System.err.println("No stream name specified");
            System.exit(1);
        }
        String streamName = args[2];
        
        if (args.length < 4) {
            System.err.println("No class name specified");
            System.exit(1);
        }
        String clazz = args[3];       
        
        Driver d = new Driver(hostName, port);
        Reader inputReader = null;
        BufferedReader br = null;
        try {
            if (!d.init()) {
                System.err.println("Driver initialization failed");
                System.exit(1);
            }
            
            if (!d.connect()) {
                System.err.println("Driver initialization failed");
                System.exit(1);           
            }
            
            inputReader = new InputStreamReader(System.in);
            br = new BufferedReader(inputReader);

            for  (String inputLine = null; (inputLine = br.readLine()) != null;) {
                String string = "{\"string\":\""+inputLine+"\"}";
                System.out.println("sending " + string);
				Message m = new Message(streamName, clazz, string);
                d.send(m);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try { d.disconnect(); } catch (Exception e) {}
            try { br.close(); } catch (Exception e) {}
            try { inputReader.close(); } catch (Exception e) {}
        }
	}
*/

    @Override
    public int delete(String arg0, String arg1) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    /**
     * TODO: A naive insert for now will only use the key parameter to send
     * to the S4 cluster
     */
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        Message m = new Message(table, "Word", key);
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
