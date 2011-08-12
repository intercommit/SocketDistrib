/*  Copyright 2011 InterCommIT b.v.
*
*  This file is part of the "SocketDistrib" project hosted on https://github.com/intercommit/SocketDistrib
*
*  SocketDistrib is free software: you can redistribute it and/or modify
*  it under the terms of the GNU Lesser General Public License as published by
*  the Free Software Foundation, either version 3 of the License, or
*  any later version.
*
*  SocketDistrib is distributed in the hope that it will be useful,
*  but WITHOUT ANY WARRANTY; without even the implied warranty of
*  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*  GNU Lesser General Public License for more details.
*
*  You should have received a copy of the GNU Lesser General Public License
*  along with Weaves.  If not, see <http://www.gnu.org/licenses/>.
*
*/
package nl.intercommit.socketdistrib;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

/**
 * Test socket server using workers that sleep (see SleepWorker).
 * See also "runtest.bat" in test resources.
 * @author frederikw
 *
 */
public class TestSleepSocket {
	
	public static Logger log = Logger.getLogger(TestSleepSocket.class);
	
	public static TestSleepSocket instance = new TestSleepSocket();
	public static int maxClients = 10;
	public static long runTime = 1000;

	public static int portNumber;
	public static Throwable clientError;
	public static boolean fromCmdLine;

	/*
	 * Change this method to test various aspects of the socket server. 
	 * See also runtest.bat in src/test/resources directory.
	 */
	public static void main(String... args) {

		fromCmdLine = (args.length > 0);
		SocketExchanger se = new SocketExchanger();
		se.minWorkers = 5;
		se.maxProcessing = (maxClients > se.minWorkers ? maxClients - 1 : se.minWorkers);
		se.lingerTimeMs = 500L;
		se.interruptWaiting = false;
		SocketAcceptor sa = new SocketAcceptor();
		sa.backLog = maxClients;
		ClientConnection[] ccs = new ClientConnection[maxClients];
		ShutdownHook sh = instance.new ShutdownHook(sa, ccs); 
		try {
			se.socketWorkerClass = SleepSocket.class;
			sa.setSocketExchanger(se);
			if (!sa.start()) return;
			se.setReporter(new SocketReporter());
			portNumber = sa.getOpenPort();
			for (int i = 0; i < maxClients; i++) {
				ClientConnection cc = instance.new ClientConnection();
				cc.reportId = "Client[" + i + "] ";
				ccs[i] = cc;
				se.execute(cc, true);
			}
			if (fromCmdLine) Runtime.getRuntime().addShutdownHook(sh);
		} catch (Throwable t) {
			log.error("Failed to start test propery.", t);
		} finally {
			if (!fromCmdLine) {
				try { Thread.sleep(runTime); } catch (Throwable ignored) {}
				sh.run();
			}
		}
	}
	
	@Test
	public void testOneConnect() {
		
		fromCmdLine = false;
		BasicConfigurator.resetConfiguration();
		final String log4jConfFile = System.getProperty("user.dir") + "/src/test/resources/log4j.properties";
		//System.out.println(log4jConfFile);
		PropertyConfigurator.configure(log4jConfFile);
		maxClients = 1;
		runTime = 100;
		SleepSocket.maxSleepTime = 10;
		SocketExchanger se = new SocketExchanger();
		SocketAcceptor sa = new SocketAcceptor();
		ClientConnection[] ccs = new ClientConnection[maxClients];
		ShutdownHook sh = instance.new ShutdownHook(sa, ccs); 
		try {
			se.socketWorkerClass = SleepSocket.class;
			sa.setSocketExchanger(se);
			if (!sa.start()) throw new RuntimeException("Socket acceptor failed to start.");
			portNumber = sa.getOpenPort();
			for (int i = 0; i < maxClients; i++) {
				ClientConnection cc = instance.new ClientConnection();
				cc.reportId = "Client[" + i + "] ";
				ccs[i] = cc;
				se.execute(cc, true);
			}
			try { Thread.sleep(runTime); } catch (Throwable ignored) {}
			sh.run();
			// Need a little extra time to make sure the client has stopped.
			try { Thread.sleep(50L); } catch (Throwable ignored) {}
			if (ccs[0].hasRunError()) throw ccs[0].getRunError();
			assertEquals("Amount of accepted and processed sockets", se.accepted.get(), se.processed.get());
		} catch (Throwable t) {
			log.error("testOneConnect failed.", t);
			throw new AssertionError("test failed with " + t);
		} finally {
			if (!sh.wasRun) sh.run();
		}

	}
	
	/**
	 * Stops the socket server and waits for workers to stop.
	 * The JVM could exit when the ShutdownHook-threads finish, which
	 * could break connections still in progress. This hook
	 * waits until normal-running the workers have finished.
	 * See also {@link SocketExchanger#close()}.
	 */
	class ShutdownHook extends Thread {
		
		Logger log = Logger.getLogger(ShutdownHook.class);
		SocketAcceptor sa;
		ClientConnection[] ccs;
		public boolean wasRun;
		
		public ShutdownHook(SocketAcceptor sa, ClientConnection[] ccs) {
			super();
			this.sa = sa;
			this.ccs = ccs;
		}
		
		public void run() {
			wasRun = true;
			// Stop the clients.
			for (int i = 0; i < maxClients; i++) try { ccs[i].interrupt(); } catch (Throwable ignored) {}
			// Stop the server.
			log.info("# Initiating shutdown ...");
			sa.stopRunning();
			while (sa.isRunning()) {
				try { Thread.sleep(50L); } catch (Throwable t) {
					log.info("Shutdown-thread interrupted while waiting for cleanup: " + t);
				}
			}
			log.info("# Shutdown complete.");
		}
	}

	/**
	 * Connects to the socket acceptor and
	 * <br> -mimics a port scan (10% chance)
	 * <br> -waits for a sleep-message from the SleepSocket. 
	 * @author frederikw
	 *
	 */
	class ClientConnection extends Interruptible {

		protected Logger log = Logger.getLogger(ClientConnection.class);
		
		public String reportId = "";
		
		private Socket s;
		private byte[] bmsg = new byte[1024];
		
		public void execute() {

			boolean OK = true;
			try {
				while (OK && !stop && !runningThread.isInterrupted()) {
					try {
						loop();
					} catch (Throwable t) {
						if (!(t instanceof InterruptedException)) {
							log.error(reportId + "got error.", t);
							OK = false;
						}
					} finally {
						if (s != null) try { s.close(); } catch (Throwable ignored) {}
					}
				}
			} finally {
				log.info(reportId + "stopped.");
			}
		}
		
		public void loop() throws UnknownHostException, IOException, InterruptedException {
			
			s = new Socket();
			s.setReuseAddress(true);
			boolean mimicPortScan = (Math.random() > 0.9);
			s.connect(new InetSocketAddress(InetAddress.getLocalHost(), portNumber), 5000);
			if (mimicPortScan) {
				int port = s.getLocalPort(); 
				s.close();
				s = null;
				log.info(reportId + "did a port scan on " + port);
				return;
			}
			log.info(reportId + "connected on port " + s.getLocalPort());
			int bread = s.getInputStream().read(bmsg);
			if (bread > 0) {
				String msg = new String(bmsg, 0, bread);
				log.info(reportId + "message received: " + msg);
				if (msg.startsWith("Too busy")) Thread.sleep(SleepSocket.maxSleepTime/2);
			} else {
				log.warn(reportId + "no message received!");
				runtimeError = new RuntimeException("Sleep message expected but not received"); 
			}
		}
	}
}
