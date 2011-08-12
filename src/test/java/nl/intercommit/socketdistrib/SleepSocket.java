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
import java.io.OutputStream;

/**
 * An example implementation of a SocketWorker.
 * The SleepWorker sleeps for a random amount of time (see maxSleepTime)
 * and then sends a message how long it slept.
 * @author frederikw
 *
 */
public class SleepSocket extends SocketWorker {

	/** Maximum time to sleep in milliseconds. */
	public static int maxSleepTime = 100;
	
	public SleepSocket() {
		super();
	}

	/**
	 * Sleeps for a random amount of time, or if tooBusy is true,
	 * sends a "too busy" message.
	 */
	@Override
	public void communicate(final boolean tooBusy) throws InterruptedException {

		try {
			final String socketId = threadName + " - " + getSocketRemoteLocation() + " - ";
			String msg = "";
			if (tooBusy) {
				msg = "Too busy to sleep.";
				log.warn(socketId + msg);
			} else {
				final long sleepTime = (long)(Math.random()*maxSleepTime);
				log.info(socketId + "socket received, sleeping " + sleepTime + " ms.");
				try { Thread.sleep(sleepTime); } catch (Throwable ignored) {
					log.info(socketId + "Interrupted while sleeping.");
				}
				log.info(socketId + "done sleeping " + sleepTime + " ms.");
				msg = "Slept for " + sleepTime + " ms.";
			}
			OutputStream out = socket.getOutputStream();
			out.write(msg.getBytes());
			out.flush();
		} catch (IOException ieo) {
			log.error("Could not report sleep message.");
		} finally {
			closeSocket();
			/* 
			 * Use this to test that hanging workers do not prevent
			 * the application from exiting. Workers should be started
			 * as daemon threads so that the JVM can kill them if they 
			 * keep hanging.
			 */
			/*
			while (true) {
				try { Thread.sleep(12000); } catch (Throwable t) {
					log.warn("final sleep interrupted: " + t);
				}
			}
			*/
		}
	}
}
