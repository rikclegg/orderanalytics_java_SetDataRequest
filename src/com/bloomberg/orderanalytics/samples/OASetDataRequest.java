/* Copyright 2017. Bloomberg Finance L.P.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:  The above
 * copyright notice and this permission notice shall be included in all copies
 * or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

package com.bloomberg.orderanalytics.samples;

import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.EventHandler;
import com.bloomberglp.blpapi.Identity;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;


public class OASetDataRequest {
	
	private static final Name 	SESSION_STARTED 		= new Name("SessionStarted");
	private static final Name 	SESSION_STARTUP_FAILURE = new Name("SessionStartupFailure");
	private static final Name 	SERVICE_OPENED 			= new Name("ServiceOpened");
	private static final Name 	SERVICE_OPEN_FAILURE 	= new Name("ServiceOpenFailure");
	private static final Name 	ERROR_INFO 				= new Name("ErrorInfo");
	private static final Name 	DATA_OPERATION_RESPONSE	= new Name("dataOperationResponse");
	private static final Name 	AUTHORIZATION_SUCCESS 	= new Name("AuthorizationSuccess");
	private static final Name 	AUTHORIZATION_FAILURE 	= new Name("AuthorizationFailure");
	private static final Name	SUBSCRIPTION_FAILURE 	= new Name("SubscriptionFailure");
	private static final Name	SUBSCRIPTION_STARTED	= new Name("SubscriptionStarted");
	private static final Name	SUBSCRIPTION_TERMINATED	= new Name("SubscriptionTerminated");
	private static final Name 	TOKEN_SUCCESS			= new Name("TokenGenerationSuccess");
	private static final Name 	TOKEN_FAILURE			= new Name("TokenGenerationFailure");

	private String 	d_authsvc;
	private String 	d_service;
	private String  d_host;
	private int     d_port;
	private String 	d_appName;
	 
	private CorrelationID requestID;
	private CorrelationID authRequestID;
	
	private Identity appIdentity;
	private String token = null;
	  
	private static boolean quit=false;
	    
	public static void main(String[] args) throws java.lang.Exception
	{
	
        System.out.println("Bloomberg - OrderAnalytics Example - SetDataRequest\n");
	
		OASetDataRequest example = new OASetDataRequest();
		example.run(args);
	
		while(!quit) {
			Thread.sleep(1);
		};
		
		System.out.println("Press any key to terminate...");
		System.in.read();
	}
  
	public OASetDataRequest()
	{
	  	
		// Define the service required, in this case the beta service, 
		// and the values to be used by the SessionOptions object
		// to identify IP/port of the back-end process.
	  	
		d_authsvc = "//blp/apiauth";
		d_service = "//blp-test/orderanalytics"; 
		d_host = "bpipe-ny-beta.bdns.bloomberg.com";
		d_port = 8196;
		d_appName = "blp-test:orderanalytics-testcli";

	}

	private void run(String[] args) throws Exception
	{

		SessionOptions d_sessionOptions = new SessionOptions();
		d_sessionOptions.setServerHost(d_host);
		d_sessionOptions.setServerPort(d_port);
		d_sessionOptions.setConnectTimeout(10000);
		
		d_sessionOptions.setAuthenticationOptions(
				"AuthenticationMode=APPLICATION_ONLY;"+
				"ApplicationAuthenticationType=APPNAME_AND_KEY;"+
				"ApplicationName=" + d_appName + ";");

		Session session = new Session(d_sessionOptions, new EMSXEventHandler());
      
		session.startAsync();
      
	}
  
	class EMSXEventHandler implements EventHandler
	{
		public void processEvent(Event event, Session session)
		{
			try {
				switch (event.eventType().intValue())
				{                
					case Event.EventType.Constants.SESSION_STATUS:
						processSessionEvent(event, session);
						break;
					case Event.EventType.Constants.SERVICE_STATUS:
						processServiceEvent(event, session);
						break;
		            case Event.EventType.Constants.TOKEN_STATUS:
		                processTokenEvent(event, session);
		                break;
					case Event.EventType.Constants.RESPONSE:
					case Event.EventType.Constants.PARTIAL_RESPONSE:
						processResponseEvent(event, session);
						break;
					case Event.EventType.Constants.AUTHORIZATION_STATUS:
						processAuthorizationEvent(event, session);
						break;
					default:
						processMiscEvents(event, session);
						break;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		private void processSessionEvent(Event event, Session session) throws Exception {

			System.out.println("Processing " + event.eventType().toString());
			MessageIterator msgIter = event.messageIterator();
			while (msgIter.hasNext()) {
				Message msg = msgIter.next();
				if(msg.messageType().equals(SESSION_STARTED)) {
					System.out.println("Session started...");
					session.openServiceAsync(d_authsvc);
				} else if(msg.messageType().equals(SESSION_STARTUP_FAILURE)) {
					System.err.println("Error: Session startup failed>> " + msg);
					quit=true;
				} else {
					System.out.println("Unprocessed message...");
					System.out.println(msg.toString());
				}
			}
		}

		private void processServiceEvent(Event event, Session session) {
			
			System.out.println("Processing " + event.eventType().toString());

			MessageIterator msgIter = event.messageIterator();
			while (msgIter.hasNext()) {
				Message msg = msgIter.next();
				if(msg.messageType().equals(SERVICE_OPENED)) {
					
					String serviceName = msg.asElement().getElementAsString("serviceName");
					
					if(serviceName==d_authsvc) {
						
						System.out.println("Authorisation Service opened...");
						try {
							session.generateToken();
						} catch(Exception e) {
							System.err.println("Failed to generate token: " + e.getMessage());
							quit=true;
						}
						
					} else if(serviceName==d_service) {
						
						sendSetDataRequest(session);
					}
					
				} else if(msg.messageType().equals(SERVICE_OPEN_FAILURE)) {
					System.err.println("Error: Service failed to open");
					System.err.println(msg.toString());
					quit=true;
				}
			}
		}
		
        private void processTokenEvent(Event event, Session session) {

			System.out.println("Processing " + event.eventType().toString());

			MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                if(msg.messageType().equals(TOKEN_SUCCESS)) {
                	token = msg.getElementAsString("token");
                	if(token==null) {
                		System.out.println("Failed to retrieve token.");
						quit=true;
                	} else {
                		sendAuthRequest(session,token);
                	}
                } else if(msg.messageType().equals(TOKEN_FAILURE)) {
                	System.err.println("Failed to generate token: " + msg.toString());
					quit=true;
                } else {
                	System.err.println("Unknown token status message: " + msg.toString());
					quit=true;
                }
            }
		}

		private void processAuthorizationEvent(Event event, Session session) {

			System.out.println("Processing: " + event.eventType().toString());
          
			MessageIterator msgIter = event.messageIterator();
			
			while(msgIter.hasNext())
			{
				Message msg = msgIter.next();
				System.out.println("AUTHORIZATION_STATUS message: " + msg.toString());
			}
			
		}
		
		private void processResponseEvent(Event event, Session session) throws Exception 
		{
			System.out.println("Processing: " + event.eventType().toString());
			
			MessageIterator msgIter = event.messageIterator();
			
			while(msgIter.hasNext())
			{
				Message msg = msgIter.next();
				
				if(msg.correlationID()==authRequestID) {
					
					if(msg.messageType().equals(AUTHORIZATION_SUCCESS)) {
						System.out.println("Authorised...Opening OrderAnalytics service...");
						session.openServiceAsync(d_service);
					} else if(msg.messageType().equals(AUTHORIZATION_FAILURE)) {
						System.out.println("Authorisation failed...");
						System.out.println(msg.toString());
						/*
						// Automatically retry after 1 second...
						Thread.sleep(1000);
						sendAuthRequest(session,token);
						*/
						quit=true;
					} else { 
						System.out.println("Unexpected authorisation message...");
						System.out.println(msg.toString());
						quit=true;
					}
					
				} else if(msg.correlationID()==requestID) {
					
					System.out.println("Message Type: " + msg.messageType());
					if(msg.messageType().equals(ERROR_INFO)) {
						Integer errorCode = msg.getElementAsInt32("ERROR_CODE");
						String errorMessage = msg.getElementAsString("ERROR_MESSAGE");
						System.out.println("ERROR CODE: " + errorCode + "\tERROR MESSAGE: " + errorMessage);
					} else if(msg.messageType().equals(DATA_OPERATION_RESPONSE)) {
						System.out.println("MESSAGE: " + msg.toString());
						System.out.println("CORRELATION ID: " + msg.correlationID());
						String channelId = msg.getElementAsString("channelId");
						long seqNum = msg.getElementAsInt64("sequenceNumber");
						Element status = msg.getElement("status");
						String statusCode = status.getElementAsString("statusCode");
						System.out.println("setDataRequest completed :-\nChannel ID: " + channelId +"\nSeqNo: " + seqNum + "\nStatus Code: " + statusCode);
					}
              	                		
					quit=true;
				} else {
					System.out.println("RESPONSE MESSAGE: " + msg.toString());
				}
			}
		}
		
		private void processMiscEvents(Event event, Session session) throws Exception 
		{
			System.out.println("Processing " + event.eventType().toString());
			MessageIterator msgIter = event.messageIterator();
			while (msgIter.hasNext()) {
				Message msg = msgIter.next();
				System.out.println("MESSAGE: " + msg);
			}
		}
		
		private void sendSetDataRequest(Session session) {
			
			Service service = session.getService(d_service);
			
            Request request = service.createRequest("setDataRequest");
            
            Element header = request.getElement("header");
            
            header.setElement("channelId","chnlid2");
            header.setElement("sequenceNumber","7867672");
            header.setElement("primaryKey","VOD LN Equity");
            header.setElement("primaryKeyType", "PKEY");
            
            Element payloadList = request.getElement("payloadList");

            Element dp1 = payloadList.appendElement();
            dp1.setElement("key", "DATA_POINT_1");
            dp1.setElement("value", "BUY");
            dp1.setElement("valueDescription", "side");
        	dp1.setElement("expireTime", "2017-02-20T00:00:00.000+00:00");
        	dp1.getElement("visualizationOption").setChoice("textVisualizationOption").setValue("POSITIVE");

        	Element dp2 = payloadList.appendElement();
            dp2.setElement("key", "DATA_POINT_2");
            dp2.setElement("value", "123.56");
            dp2.setElement("valueDescription", "targetprice");
        	dp2.setElement("expireTime", "2017-02-20T00:00:00.000+00:00");
        	dp2.getElement("visualizationOption").setChoice("textVisualizationOption").setValue("NEGATIVE");

    	    System.out.println("Request: " + request.toString());

            requestID = new CorrelationID();
            
            // Submit the request
        	try {
                session.sendRequest(request,appIdentity,requestID);
        	    System.out.println("Request sent.");
        	} catch (Exception ex) {
        		System.err.println("Failed to send the request");
        		quit=true;
        	}
		}
		
		private void sendAuthRequest(Session session, String token)
		{
    		Service authService = session.getService(d_authsvc);
    		Request authReq = authService.createAuthorizationRequest();
    		authReq.set("token", token);
    		appIdentity = session.createIdentity();
    		authRequestID = new CorrelationID();
    		try {
				session.sendAuthorizationRequest(authReq, appIdentity,authRequestID);
			} catch (Exception e) {
				System.out.println("Unable to send authorization request: " + e.getMessage());
				quit=true;
			}
    	}
	}
}
