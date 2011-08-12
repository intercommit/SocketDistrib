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

import org.apache.log4j.Logger;

/**
 * A base class for a runnable that keeps track
 * of interrupts and stop-requests.
 * @author frederikw
 *
 */
public class Interruptible implements Runnable {
	
	protected Logger log = Logger.getLogger(getClass());

	protected volatile boolean stop;
	protected volatile Thread runningThread;
	protected Throwable runtimeError;
	protected InterruptedException interruptedException;
	protected String threadName = "";
	
	/** 
	 * Calls the execute method within a try-catch-finally block so
	 * that the status of this runnable is accurate and any
	 * runtime-error is cached and logged as error.
	 */
	public final void run() {
		
		runningThread = Thread.currentThread();
		String threadNameOriginal = runningThread.getName();
		if (threadNameOriginal == null) threadNameOriginal = "T" + getClass().getSimpleName();
		if (threadName != null && !threadName.isEmpty()) runningThread.setName(threadName);
		else threadName = threadNameOriginal;
		try {
			execute();
		} catch (InterruptedException ie) {
			interruptedException = ie;
		} catch (Throwable t) {
			runtimeError = t;
		} finally {
			if (threadNameOriginal != null && runningThread != null) runningThread.setName(threadNameOriginal);
			runningThread = null;
			if (log.isDebugEnabled()) {
				log.debug(toString());
			}
			if (runtimeError != null) {
				log.error(threadName + " stopped after unexpected error.", runtimeError);
			}
		}
	}
	
	/** 
	 * Called by the run-method to execute a task. 
	 * Use "stop" and "runningThread.isInterrupted()" to 
	 * determine when to stop execution.
	 * @throws InterruptedException always re-throw this exception, do not hide it.
	 */
	public void execute() throws InterruptedException {}

	/** The name of the runningThread (optional). */
	public void setThreadName(String tname) { threadName = tname; }
	/** Stops the execution after the current task has finished. */
	public void stopRunning() { stop = true; }
	/** True if stopRunning() was called. */
	public boolean wasStopped() { return stop; }
	/** Returns true if a thread is executing methods of this object. */
	public boolean isRunning() { return (runningThread != null); }
	/** Returns null or the thread that is executing methods of this object. */
	public Thread getRunningThread() { return runningThread; }
	/** Interrupts the current task and stops the execution. */
	public void interrupt() {
		Thread t = runningThread;
		if (t != null) {
			stop = true;
			t.interrupt();
		}
	}
	/** Returns true if an interruptedException was caughed. */
	public boolean isInterrupted() { return (interruptedException != null); }
	/** Returns the interrupted exception or null. */
	public InterruptedException getInterrupted() { return interruptedException; }
	/** True if an error other then an interrupted exception stopped the runnable. */
	public boolean hasRunError() { return (runtimeError != null); }
	/** The error which caused the runnable to stop, other then an interrupted exception. */
	public Throwable getRunError() { return runtimeError; }
	/** Provides a description and status of this Interruptible. */
	public String toString() {
		
		final StringBuilder sb = new StringBuilder(getClass().getName());
		sb.append(" - ").append(threadName).append(": ");
		sb.append("stopped=").append(wasStopped());
		sb.append(", running=").append(isRunning());
		sb.append(", interrupted=").append(isInterrupted());
		if (interruptedException != null) sb.append(", interrupted-description=").append(interruptedException);
		sb.append(", runerror=").append(hasRunError());
		if (runtimeError != null) sb.append(", runerror-description=").append(runtimeError);
		return sb.toString();
	}
}
