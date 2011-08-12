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

/**
 * Logs the amount of connections accepted, processing and processed at regular intervals.
 * @author frederikw
 *
 */
public class SocketReporter extends Interruptible {

	protected SocketExchanger se;

	protected long lastAccepted;
	protected long lastProcessing;
	protected long lastProcessed;
	protected int lastWorkers;

	/** How often to report (period in milliseconds, default 10 seconds). */
	public long reportInterval = 10000L;

	/** 
	 * An optional socket-reporter ID shown in the report-log statement.
	 * Useful in case there are multiple socket-reporters running. 
	 */
	public String reportId = "";


	/** Called by socket exchanger before the reporter is started. */
	public void setSocketExchanger(SocketExchanger se) {
		this.se = se;
	}

	/**
	 * Reports in a loop, but only when there are changes in the reported numbers.
	 */
	@Override
	public void execute() throws InterruptedException {

		try {
			while (!stop && !runningThread.isInterrupted()) {
				boolean report = (se.accepted.get() != lastAccepted);
				if (!report) report = (se.processed.get() != lastProcessed);
				if (!report) report = (se.processing.get() != lastProcessing);
				if (!report) report = (se.getWorkerAmount() != lastWorkers);
				if (report) log.info(getReport());
				Thread.sleep(reportInterval);
			}
		} finally {
			log.info(getReport());
			log.info(reportId + "Connections reporter stopping.");
		}
	}

	/** Creates a report for the log and updates the "lastCount" values. */
	protected String getReport() {

		StringBuilder sb = new StringBuilder(128);

		if (reportId != null && !reportId.isEmpty()) sb.append(reportId).append(" - ");

		long count = se.accepted.get();
		sb.append("accepted: ").append(count - lastAccepted).append(", ");
		lastAccepted = count;

		count = se.processing.get();
		sb.append("processing: ").append(count).append(", ");
		lastProcessing = count;

		count = se.processed.get();
		sb.append("processed: ").append(count - lastProcessed);
		lastProcessed = count;

		if (lastWorkers != se.getWorkerAmount()) {
			lastWorkers = se.getWorkerAmount();
			sb.append(", workers: ").append(lastWorkers);
		}
		return sb.toString();
	}
}
