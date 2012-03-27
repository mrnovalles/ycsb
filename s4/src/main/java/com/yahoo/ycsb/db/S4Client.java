/**
 * S4 client binder for YCSB - 1st try
 * 
 * */

package com.yahoo.ycsb.db;

import org.apache.s4.client.Driver;
import org.apache.s4.client.Message;
import org.apache.s4.ft.TestUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;

import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

public class S4Client extends com.yahoo.ycsb.DB {
    
    public static final String HOST_PROPERTY = "s4.host";
    public static final String PORT_PROPERTY = "s4.port";
    public static final String N_PES = "s4.npes" ;
    public static int numPE;
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
        
        numPE = Integer.parseInt(props.getProperty(N_PES));
        if ( numPE <= 0){
            numPE = 100;
        }
       
		
    }
    public void cleanup(){
        try { 
        	d.disconnect();
        	TestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);
        }
        catch (Exception e) {}
		
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

    	Random rnd = new Random();
    	
    	//Note: the . specifies a sentence being sent
    	String string = String.valueOf(rnd.nextInt(numPE));
    	String str = "{\"string\":\""+ string +"."+"\"}";

    	/* TODO: RawWords should be a command line parameter, may be table name
	  	Closely related to the deployed application.	*/
    	Message m = new Message("RawWords","test.s4.Word", str);
    	ZooKeeper zk;
    	try {
    		zk = TestUtils.createZkClient();
    		CountDownLatch checkpoint = new CountDownLatch(1);
    		TestUtils.watchAndSignalCreation("/"+string, checkpoint, zk);
    		d.send(m);
    		return 0;
    	} catch (IOException e) {
    		e.printStackTrace();
    		return 1;
    	} catch (KeeperException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	} catch (InterruptedException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
    	return 1;
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
