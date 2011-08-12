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
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.Semaphore;

/**
 * A thread that handles a connection.
 * The SocketWorker will block until a socket is available from the SocketExchanger.
 * If a socket is not available within SocketExchanger.lingerTimeMs, the worker (thread) dies.
 * @author frederikw
 *
 */
public class SocketWorker extends Interruptible {

	/** 
	 * When the Worker is in the socket exchanger's queue, this semaphore
	 * is used to indicate when the socket exchanger has made a socket available.
	 */
	protected Semaphore socketAvailable = new Semaphore(0); 
	protected SocketExchanger se;
	protected volatile Socket socket;
	public long workerNumber;

	/** Called by socket-exchanger to initialize this worker. */
	public void setSocketExchanger(SocketExchanger se) {
		this.se = se;
		workerNumber = se.workerCount.incrementAndGet();
		setThreadName(getClass().getSimpleName() + "-" + workerNumber);
		if (log.isDebugEnabled()) log.debug("New worker " + threadName + " created");
	}
	
	/** 
	 * Called by socket-exchanger when this socketWorker is selected to handle an accepted socket.
	 * @param init set to true when the socketWorker should not wait for a release
	 * (which is the case when this socketWorker is created and should handle the
	 * given socket immediately). 
	 * */
	public void setSocket(final Socket s, final boolean init) { 
		this.socket = s;
		if (!init) socketAvailable.release();
	}
	
	/**
	 * Accepts sockets in a loop until interrupted or no longer needed
	 * (see also {@link SocketExchanger#lingerTimeMs}.
	 * Calls the communicate-methods when a socket is available and valid.
	 * Updates the counters in the socket-exchanger.
	 */
	@Override
	public final void execute() throws InterruptedException {
		
		try {
			while (!stop) {
				if (socket == null && se.offer(this, se.lingerTimeMs)) {
					socketAvailable.acquire();
				}
				if (socket == null) { if (!stop) stop = (se.getWorkerAmount() > se.minWorkers); continue; }
				if (!isValidSocket()) { closeSocket(); se.processed.incrementAndGet(); continue; }
				se.processing.incrementAndGet();
				communicate(se.processing.get() > se.maxProcessing);
				socket = null;
				se.processing.decrementAndGet();
				se.processed.incrementAndGet();
			}
		} finally {
			if (socket != null) {
				se.processing.decrementAndGet();
				se.processed.incrementAndGet();
				closeSocket();
			}
			if (!se.removeWorker(this)) {
				log.warn("Socket worker " + threadName + " was not registered as worker.");
			}
		}
	}
	
	/** 
	 * Overwrite this method to handle a new connection. 
	 * If the socket is to be closed, this method must do so (close the socket in a finally block if needed).
	 * @param tooBusy true when the maximum amount of threads processing connections has been exceeded.  
	 * @throws InterruptedException
	 */
	public void communicate(final boolean tooBusy) throws InterruptedException {
		if (tooBusy) {
			log.warn("Too busy to handle socket connection from " + getSocketRemoteLocation());
		} else {
			log.info("Received a socket connection from " + getSocketRemoteLocation());
		}
		closeSocket();
	}
	
	/**
	 * Returns remote-IP-address:remote-port, or, if the socket is not connected,
	 * "0.0.0.0:-1".
	 */
	public String getSocketRemoteLocation() {
		final InetAddress ia = socket.getInetAddress();
		return (ia == null ? "0.0.0.0:-1" : ia.getHostAddress() + ":" + socket.getPort());
	}

	/**
	 * Called by execute-method to check if accepted socket is valid.
	 * If this method returns false, the socket is closed immediately.
	 * This can be useful to detect port-scans etc.
	 * @return false when the socket is closed or not connected, true otherwise. 
	 */
	protected boolean isValidSocket() {
		
		boolean valid = true;
		if (socket.isClosed()) {
			log.info("Received a socket that was already closed.");
			valid = false;
		} else if (!socket.isConnected()) {
			log.info("Received a socket that was not connected.");
			valid = false;
		}
		return valid;
	}
	
	/** Closes the socket, logs a warning when it fails. */
	protected void closeSocket() {
		
		if (socket != null && !socket.isClosed()) {
			try { socket.close(); } catch (IOException ioerror) {
				log.warn("Could not properly close socket.", ioerror);
			}
		}
		socket = null;
	}
}
