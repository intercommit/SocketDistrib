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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

import org.apache.log4j.Logger;

/**
 * A ServerSocket-thread that accepts incoming sockets and hands them over to the SocketExchanger.
 * @author frederikw
 *
 */
public class SocketAcceptor extends Interruptible {

	protected Logger log = Logger.getLogger(getClass());

	/** The port-number to listen on for incoming sockets. Default 0 (which means "any available port"). */  
	public int portNumber;
	/** The amount of sockets that can be waiting to be accepted. Default 50, 
	 * set higher to prevent "connection refused" errors.
	 */
	public int backLog = 50;
	/** When an unexpected error occurs and this variable is true, System.exit(1) will be called
	 * (which triggers any registered shutdown-hooks to be executed). */
	public boolean exitOnError;

	protected SocketExchanger socketExchanger;
	protected ServerSocket serverSocket;
	
	private int openPort;

	/** Required to handle accepted sockets. */
	public void setSocketExchanger(SocketExchanger se) { this.socketExchanger = se; }
	/** The port on which the server socket is listening. */
	public int getOpenPort() { return openPort; } 
	
	/** 
	 * This will open the server-socket on portNumber (no sockets are accepted yet).
	 * @return true if the server-socket was opened and ready to accept incoming socket connections.
	 */
	public boolean openSocketServer() {
		
		boolean OK = false;
		serverSocket = null;
		try {
			serverSocket = new ServerSocket();
			// See http://meteatamel.wordpress.com/2010/12/01/socket-reuseaddress-property-and-linux/
			serverSocket.setReuseAddress(true);
			serverSocket.bind(new InetSocketAddress(portNumber), backLog);
			serverSocket.setSoTimeout(0);
			openPort = serverSocket.getLocalPort();
			log.info("Listening for socket connections on port " + openPort);
			threadName = getClass().getSimpleName() + ":" + openPort;
			OK = true;
		} catch (Throwable t) {
			log.error("Could not open the server socket.", t);
			closeServerSocket();
			serverSocket = null;
			openPort = 0;
		}
		return OK;
	}
	
	/** 
	 * Starts accepting incoming socket connections. 
	 * <br>A SocketExchanger must have already been set.
	 * <br>Before starting this method, you should call openSocketServer() and check the returned value.
	 * If openSocketServer() was not called previously, it is called by this method.
	 * <br>At least one SocketWorker is started before this SocketAcceptor is started.
	 * @return true if this SocketAcceptor started, false otherwise. 
	 */
	public boolean start() {

		if (socketExchanger == null) {
			log.error("A socket-exchanger is required.");
		}
		if (serverSocket == null && !openSocketServer()) {
			return false;
		}
		int initWorkers = socketExchanger.minWorkers;
		if (initWorkers < 1) initWorkers = 1;
		boolean initError = false;
		try {
			for (int i = 0; i < initWorkers; i++) 
				socketExchanger.execute(socketExchanger.getWorker(null), true);
		} catch (Exception e) {
			initError = true;
			log.error("Failed to instantiate a socket worker.", e);
		} finally {
			if (initError) closeServerSocket();
		}
		if (initError) return false;
		socketExchanger.execute(this, false);
		return true;
	}

	/** The run-loop, should not be called directly (use start() method). */
	@Override
	public void execute() {

		Socket s = null;
		try {
			while (!stop && !runningThread.isInterrupted()) {
				try {
					s = serverSocket.accept();
				} catch (SocketException se) {
					log.info("Received a shutdown request: " + se);
					stop = true;
				} catch (IOException ioe) {
					log.error("IO-error while waiting for new socket connection.", ioe); 
					// TODO: Check if server-socket should be restarted, see also
					// http://stackoverflow.com/questions/3028543/what-to-do-when-serversocket-throws-ioexception-and-keeping-server-running
				}
				if (s == null) continue;
				socketExchanger.offer(s);
				s = null;
			}
		} catch (InterruptedException ie) {
			// Can happen when socketExchanger.offer(s) is blocking.
			log.info("Socket server interrupted.");
		} catch (Throwable t) {
			// Should this be fatal?
			log.error("Unexpected error encountered, socket server stopping!", t);
			runtimeError = t;
		} finally {
			closeServerSocket();
			// if an InterruptedException occurred a new socket might have been left dangling.
			closeSocket(s);
			socketExchanger.close();
			if (exitOnError && runtimeError != null) {
				runningThread = null;
				System.exit(1);
			}
		}
	}

	/** 
	 * Stops the server socket and all related workers. 
	 * After first call the subsequent calls have no effect.
	 * This call is non-blocking, to see an example of how to wait for all
	 * threads to stop, look at {@link TestSleepSocket.ShutdownHook}
	 */
	public void stopRunning() { 
		stop = true;
		/*
		 * The execute-loop may or may not have stopped (depending if a socket
		 * is being handled or serverSocket is waiting for a socket).
		 * If it has stopped, closeServerSocket() is already called,
		 * if it did not stop, closeServerSocket() will surely stop the loop.   
		 */
		closeServerSocket();
	}
	
	/** 
	 * Closes the server-socket.
	 * This will trigger the execute-method to stop and close all workers (via the socketExchanger.close() method).
	 * This method is synchronized, after first call the subsequent calls have no effect.
	 */
	public synchronized void closeServerSocket() {

		if (serverSocket != null && !serverSocket.isClosed()) {
			if (log.isDebugEnabled()) log.debug("Closing socket server on port " + openPort);
			try {serverSocket.close(); }
			catch (Throwable t) { log.warn("Could not properly close server socket.", t); }
			log.info("Server socket on port " + openPort + " closed. Number of accepted sockets: " + socketExchanger.accepted.get());
		}
		serverSocket = null;
		openPort = 0;
	}
	
	protected void closeSocket(final Socket s) {
		if (s != null && !s.isClosed()) {
			try { s.close(); } catch (IOException ioerror) {
				log.warn("Could not properly close socket.", ioerror);
			}
		}
	}
}
