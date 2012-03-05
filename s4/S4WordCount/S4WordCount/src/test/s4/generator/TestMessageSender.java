package test.s4.generator;

import io.s4.client.Driver;
import io.s4.client.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

public class TestMessageSender {
	public static void main(String[] args) {
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
}
