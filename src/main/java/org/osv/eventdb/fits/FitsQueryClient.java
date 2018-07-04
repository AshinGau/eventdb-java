package org.osv.eventdb.fits;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.osv.eventdb.hbase.MDQuery;

public abstract class FitsQueryClient {
	private MDQuery mdQuery;
	private int evtLength;

	protected abstract FitsEvent getEvt(byte[] bin);

	public FitsQueryClient(String tableName, int evtLength) throws IOException {
		mdQuery = new MDQuery(tableName);
		this.evtLength = evtLength;
	}

	public FitsQueryClient(MDQuery mdQuery, int evtLength) {
		this.mdQuery = mdQuery;
		this.evtLength = evtLength;
	}

	public List<byte[]> query(FitsQueryFormater format) throws IOException {
		LinkedList<byte[]> result = (LinkedList<byte[]>) mdQuery.query(format.buckets, format.getOp, format.scanOp);
		byte[] evtBin = new byte[evtLength];
		double startTime = format.startTime;
		double endTime = format.endTime;
		if (result.size() == 1) {
			byte[] bytes = result.getFirst();
			int count = bytes.length / evtLength;
			int start = -1;
			int end = -1;
			for (int i = 0; i < count; i++) {
				int index = i * evtLength;
				for (int j = 0; j < evtLength; j++)
					evtBin[j] = bytes[index + j];
				FitsEvent event = getEvt(evtBin);
				if (event.getTime() >= startTime) {
					start = i;
					break;
				}
			}
			for (int i = count - 1; i >= 0; i--) {
				int index = i * evtLength;
				for (int j = 0; j < evtLength; j++)
					evtBin[j] = bytes[index + j];
				FitsEvent event = getEvt(evtBin);
				if (event.getTime() <= endTime) {
					end = i;
					break;
				}
			}
			if (start != -1 && end != -1) {
				int firstBucketBytes = (end - start + 1) * evtLength;
				int startIndex = start * evtLength;
				byte[] firstBucket = new byte[firstBucketBytes];
				for (int i = 0; i < firstBucketBytes; i++)
					firstBucket[i] = bytes[startIndex + i];
				result.removeFirst();
				result.add(firstBucket);
			} else {
				result.removeFirst();
			}
		} else if (result.size() > 1) {
			byte[] firstBytes = result.getFirst();
			byte[] lastBytes = result.getLast();

			int firstCount = firstBytes.length / evtLength;
			int firstStart = -1;
			for (int i = 0; i < firstCount; i++) {
				int index = i * evtLength;
				for (int j = 0; j < evtLength; j++)
					evtBin[j] = firstBytes[index + j];
				FitsEvent event = getEvt(evtBin);
				if (event.getTime() >= startTime) {
					firstStart = i;
					break;
				}
			}
			if (firstStart != -1) {
				int firstBucketLength = (firstCount - firstStart) * evtLength;
				byte[] firstBucket = new byte[firstBucketLength];
				int firstStartIndex = firstStart * evtLength;
				for (int i = 0; i < firstBucketLength; i++)
					firstBucket[i] = firstBytes[firstStartIndex + i];
				result.removeFirst();
				result.addFirst(firstBucket);
			} else {
				result.removeFirst();
			}

			int lastCount = lastBytes.length / evtLength;
			int lastStart = -1;
			for (int i = lastCount - 1; i >= 0; i--) {
				int index = i * evtLength;
				for (int j = 0; j < evtLength; j++)
					evtBin[j] = lastBytes[index + j];
				FitsEvent event = getEvt(evtBin);
				if (event.getTime() <= endTime) {
					lastStart = i;
					break;
				}
			}
			if (lastStart != -1) {
				int lastBucketLength = (lastStart + 1) * evtLength;
				byte[] lastBucket = new byte[lastBucketLength];
				for (int i = 0; i < lastBucketLength; i++)
					lastBucket[i] = lastBytes[i];
				result.removeLast();
				result.addLast(lastBucket);
			} else {
				result.removeLast();
			}
		}

		return result;
	}

	public List<byte[]> query(String query) throws IOException {
		FitsQueryFormater format = new HeQueryFormater(query);
		LinkedList<byte[]> result = (LinkedList<byte[]>) mdQuery.query(format.buckets, format.getOp, format.scanOp);
		byte[] evtBin = new byte[evtLength];
		double startTime = format.startTime;
		double endTime = format.endTime;
		if (result.size() == 1) {
			byte[] bytes = result.getFirst();
			int count = bytes.length / evtLength;
			int start = -1;
			int end = -1;
			for (int i = 0; i < count; i++) {
				int index = i * evtLength;
				for (int j = 0; j < evtLength; j++)
					evtBin[j] = bytes[index + j];
				FitsEvent event = getEvt(evtBin);
				if (event.getTime() >= startTime) {
					start = i;
					break;
				}
			}
			for (int i = count - 1; i >= 0; i--) {
				int index = i * evtLength;
				for (int j = 0; j < evtLength; j++)
					evtBin[j] = bytes[index + j];
				FitsEvent event = getEvt(evtBin);
				if (event.getTime() <= endTime) {
					end = i;
					break;
				}
			}
			if (start != -1 && end != -1) {
				int firstBucketBytes = (end - start + 1) * evtLength;
				int startIndex = start * evtLength;
				byte[] firstBucket = new byte[firstBucketBytes];
				for (int i = 0; i < firstBucketBytes; i++)
					firstBucket[i] = bytes[startIndex + i];
				result.removeFirst();
				result.add(firstBucket);
			} else {
				result.removeFirst();
			}
		} else if (result.size() > 1) {
			byte[] firstBytes = result.getFirst();
			byte[] lastBytes = result.getLast();

			int firstCount = firstBytes.length / evtLength;
			int firstStart = -1;
			for (int i = 0; i < firstCount; i++) {
				int index = i * evtLength;
				for (int j = 0; j < evtLength; j++)
					evtBin[j] = firstBytes[index + j];
				FitsEvent event = getEvt(evtBin);
				if (event.getTime() >= startTime) {
					firstStart = i;
					break;
				}
			}
			if (firstStart != -1) {
				int firstBucketLength = (firstCount - firstStart) * evtLength;
				byte[] firstBucket = new byte[firstBucketLength];
				int firstStartIndex = firstStart * evtLength;
				for (int i = 0; i < firstBucketLength; i++)
					firstBucket[i] = firstBytes[firstStartIndex + i];
				result.removeFirst();
				result.addFirst(firstBucket);
			} else {
				result.removeFirst();
			}

			int lastCount = lastBytes.length / evtLength;
			int lastStart = -1;
			for (int i = lastCount - 1; i >= 0; i--) {
				int index = i * evtLength;
				for (int j = 0; j < evtLength; j++)
					evtBin[j] = lastBytes[index + j];
				FitsEvent event = getEvt(evtBin);
				if (event.getTime() <= endTime) {
					lastStart = i;
					break;
				}
			}
			if (lastStart != -1) {
				int lastBucketLength = (lastStart + 1) * evtLength;
				byte[] lastBucket = new byte[lastBucketLength];
				for (int i = 0; i < lastBucketLength; i++)
					lastBucket[i] = lastBytes[i];
				result.removeLast();
				result.addLast(lastBucket);
			} else {
				result.removeLast();
			}
		}

		return result;
	}
}