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

import java.lang.Thread.State;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

/**
 * An object that hands over an accepted socket from the SocketAcceptor
 * to a SocketWorker ready to handle a socket.
 * If a SocketWorker is not available within qTimeOut, a SocketWorker is created and started.
 * Various variables in this object are used for reporting and blocking threads
 * (SocketAcceptor and SocketWorker run in a thread). 
 * @author frederikw
 *
 */
public class SocketExchanger {
	
	protected Logger log = Logger.getLogger(getClass());

	/** Number of accepted sockets that have a worker assigned. */
	public final AtomicLong accepted = new AtomicLong();
	/** Number of workers that are working. */
	public final AtomicLong processing = new AtomicLong();
	/** Number of sockets that have been processed. */
	public final AtomicLong processed = new AtomicLong();
	/** The amount of workers created, used by SocketWorker to get a unique number for each worker. */
	public final AtomicLong workerCount = new AtomicLong();

	/** 
	 * Time to wait (in milliseconds) for a worker to become available to handle a socket.
	 * A new worker is created when an existing worker is not available within the time-out.
	 * Default 5 (which means that max. 200 new workers can be created per second).
	 * Must be at least 1 and much less then lingerTimeMs.
	 */
	public long qTimeOut = 5L;
	/** 
	 * Maximum amount of sockets that can be processed concurrently.
	 * This is a soft-limit: when this limit is reached, (new) workers receive a socket
	 * with the "tooBusy" parameter set to true.
	 * Default 42 (must be at least 1 more then minWorkers).
	 */
	public int maxProcessing = 42;
	/**
	 * The minimum amount of workers always waiting to handle a socket.
	 * These workers are creating during startup and die only when the SocketAccpetor is closed.
	 * Default 1 (which is the minimum value).
	 */
	public int minWorkers = 1;
	/**
	 * The class handling accepted sockets.
	 * If setting a different class (which must have a default constructor)
	 * does not work for your implementation, overwrite the getWorker method,
	 * copy the contents of this class' getWorker method and modify it as needed
	 * (and do NOT call super.getWorker).
	 */
	public Class<? extends SocketWorker> socketWorkerClass = SocketWorker.class;

	/**
	 * The time (in milliseconds) a worker will wait for a socket before dying,
	 * unless the amount of workers is lower then minWorkers.
	 * The higher this number, the less new threads will be created 
	 * and the longer existing threads will stay alive (in waiting state when there is no work). 
	 * Default 30 000. Should be much more then qTimeOut.
	 */
	public long lingerTimeMs = 30000L;

	/**
	 * The blocking exchange-queue of size one that is used to hand-over sockets to socket workers.
	 */
	protected final BlockingQueue<SocketWorker> sq = new LinkedBlockingQueue<SocketWorker>(1);
	
	/**
	 * The reporter that logs information about the amount of sockets accepted, being processed and processed.
	 * Optional, does not have to be set.
	 */
	protected SocketReporter sr;
	
	/** Concurrent unbounded queue containing workers that are running. */
	private final ConcurrentLinkedQueue<Interruptible> workers = new ConcurrentLinkedQueue<Interruptible>();
	/** The amount of workers running. Calling workers.size() is expensive, use this variable instead. */
	private final AtomicInteger workerAmount = new AtomicInteger();

	/** Starts the given socket-reporter. */
	public void setReporter(SocketReporter sr) {
		this.sr = sr;
		sr.setSocketExchanger(this);
		execute(sr, true);
	}
	
	/** Creates a new socket worker ready to process the given socket. */
	public SocketWorker getWorker(final Socket s) throws InstantiationException, IllegalAccessException {
		
		SocketWorker sw = socketWorkerClass.newInstance();
		sw.setSocketExchanger(this);
		sw.setSocket(s, true);
		addWorker(sw);
		return sw;
	}
	
	/** Adds a worker to the list of running workers. */
	protected void addWorker(final SocketWorker sw) {
		workers.add(sw);
		workerAmount.incrementAndGet();
	}
	/** Removes a worker from the list of running workers. */
	public boolean removeWorker(final SocketWorker sw) {
		boolean removed = workers.remove(sw);
		if (removed) workerAmount.decrementAndGet();
		return removed;
	}
	/** The amount of running workers. */
	public int getWorkerAmount() {
		return workerAmount.get();
	}
	
	/** 
	 * Starts a runnable. 
	 * @param daemon should be true for (socket) workers 
	 * so that hanging workers do not prevent the application from shutting down.
	 */
	public void execute(Runnable r, boolean daemon) { 
		
		Thread t = new Thread(r);
		t.setDaemon(daemon);
		t.start();
	}
	
	/**
	 *  Called by SocketAcceptor to handle an accepted socket.
	 *  Creates a new SocketWorker when none is available within qTimeOut. 
	 */
	public void offer(final Socket s) throws InterruptedException, InstantiationException, IllegalAccessException {

		final SocketWorker sw = sq.poll(qTimeOut, TimeUnit.MILLISECONDS);
		if (sw == null) {
			execute(getWorker(s), true);
		} else {
			sw.setSocket(s, false);
		}
		accepted.incrementAndGet();
	}
	
	/** Called by SocketWorker when it is ready to accept a socket. */
	public boolean offer(final SocketWorker sw, final long timeOutMs) throws InterruptedException {
		return sq.offer(sw, timeOutMs, TimeUnit.MILLISECONDS);
	}
	
	/** 
	 * When closing, the time to wait in milliseconds before interrupting running workers, 
	 * after setting stop to true for the workers.
	 * <br>Default 5000. 
	 */
	public long workersStopTime = 5000L;
	/** 
	 * When closing, workers in wait-state will be interrupted immediately if this is true.
	 * <br>Default false. 
	 */
	public boolean interruptWaiting = false;
	/** 
	 * The maximum time to wait in milliseconds after interrupting running workers that did not stop within workersStopTime. 
	 * <br>Default 1000.
	 */
	public long workersShutDownTime = 1000L;

	/** 
	 * Called by SocketAcceptor, stops all socket-workers and the socket reporter (when available). 
	 * <br>First, stop is set to true and waiting workers are interrupted if interruptWaiting is true.
	 * <br>Then workersStopTime is waited for workers to close.
	 * <br>After that any running workers are interrupted
	 * <br>and workersShutDownTime is waited before exiting this method.
	 * <br>Before this method exits any workers that are still running 
	 * are shown in an error-log statement. 
	 */
	public void close() {
	
		// Signal stop to all workers.
		Interruptible[] runners = workers.toArray(new Interruptible[0]);
		for(Interruptible i : runners) {
			Thread t = i.getRunningThread();
			if (t == null) continue;
			i.stopRunning();
			if (interruptWaiting) {
				// Interrupt workers in wait-state.
				Thread.State ts = t.getState();
				if (ts == State.BLOCKED || ts == State.TIMED_WAITING || ts == State.WAITING) {
					i.interrupt();
				}
			}
		}
		// Interrupt all workers waiting for a socket.
		try {
			Interruptible sw = sq.poll(1L, TimeUnit.MILLISECONDS);
			while (sw != null) {
				sw.interrupt();
				sw = sq.poll(1L, TimeUnit.MILLISECONDS);
			}
		} catch (InterruptedException ie) {
			log.warn("Interrupted while closing socket workers waiting for a socket.");
		}
		// Stop the socket reporter.
		if (sr != null) { sr.stopRunning(); sr.interrupt(); }
		int openRunners = workersRunning();
		if (openRunners == 0) {
			log.info("All workers stopped, total processed: " + processed.get());
			return;
		}
		log.info("Waiting for " + openRunners + " workers to finish.");
		long tstart = System.currentTimeMillis();
		while (openRunners > 0 && System.currentTimeMillis() < tstart + workersStopTime) {
			try { Thread.sleep(50L); } catch (Throwable ignored) {}
			openRunners = workersRunning();
		}
		if (openRunners == 0) {
			log.info("All workers stopped, total processed: " + processed.get());
			return;
		}
		log.warn("Forcing " + openRunners + " workers to stop.");
		runners = workers.toArray(new Interruptible[0]);
		for(Interruptible i : runners) i.interrupt();
		tstart = System.currentTimeMillis();
		while (openRunners > 0 && System.currentTimeMillis() < tstart + workersShutDownTime) {
			try { Thread.sleep(50L); } catch (Throwable ignored) {}
			openRunners = workersRunning();
		}
		runners = workers.toArray(new Interruptible[0]);
		if (runners.length == 0) {
			log.info("All workers stopped, total processed: " + processed.get());
			return;
		}
		StringBuilder sb = new StringBuilder("Could not stop ").append(runners.length).append(" workers:\n");
		for(Interruptible i : runners) { sb.append(i.toString()).append("\n"); }
		sb.append("processing: ").append(processing.get()).append("\n");
		sb.append("total accepted: ").append(accepted.get()).append(", total processed: " + processed.get());
		log.error(sb.toString());
	}
	
	/**
	 * Returns the number of workers that are still running.
	 */
	protected int workersRunning() {
		
		int running = 0;
		Interruptible[] runners = workers.toArray(new Interruptible[0]);
		int i = 0;
		while (i < runners.length) {
			if (runners[i].isRunning()) running++;
			i++;
		}
		return running;
	}
}
