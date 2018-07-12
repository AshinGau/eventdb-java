package org.osv.eventdb.fits.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.osv.eventdb.fits.FitsEventDBClient;
import org.osv.eventdb.fits.HeQueryClient;

import fi.iki.elonen.NanoHTTPD;
import fi.iki.elonen.NanoHTTPD.Response.Status;

public class RestServer extends NanoHTTPD {
	public RestServer(int port) throws IOException {
		super(port);
		start(NanoHTTPD.SOCKET_READ_TIMEOUT, false);
		System.out.println("RestFul Server is running at http://localhost:" + port + "\n");
	}

	public static void runAtPort(int port) {
		try {
			client = new FitsEventDBClient();
			new RestServer(port);
		} catch (IOException ioe) {
			System.err.println("Couldn't start server:\n" + ioe);
		}
	}

	public static Pattern HE_QUERY = Pattern.compile("/he/(\\w+)");
	public static Pattern STATIC_SRC = Pattern.compile("/static/(.+)");
	public static Map<String, String> MIME_TYPE = NanoHTTPD.mimeTypes();
	public static String EVENTDB_ENV = System.getenv("EVENTDB_ENV");
	public static String EVENTDB_HOME = System.getenv("EVENTDB_HOME");
	public static FitsEventDBClient client;

	public static InputStream getResourceAsStream(String path) throws IOException {
		if ("debug".equals(EVENTDB_ENV)) {
			String staticRoot = EVENTDB_HOME + "/src/main/resources/";
			return new FileInputStream(staticRoot + path);
		} else {
			return RestServer.class.getClassLoader().getResourceAsStream(path);
		}
	}

	public static Response clientGetTableList() throws IOException {
		StringBuffer sb = new StringBuffer();
		String[] tables = client.list();
		if (tables.length > 0)
			sb.append(tables[0]);
		for (int i = 1; i < tables.length; i++)
			sb.append("," + tables[i]);
		return newFixedLengthResponse(Status.OK, NanoHTTPD.MIME_PLAINTEXT, sb.toString());
	}

	public static Response clientGetTotalEvents() throws IOException {
		return newFixedLengthResponse(Status.OK, NanoHTTPD.MIME_PLAINTEXT, String.valueOf(client.getTotalEvents()));
	}

	public static Response clientGetTotalFiles() throws IOException {
		return newFixedLengthResponse(Status.OK, NanoHTTPD.MIME_PLAINTEXT, String.valueOf(client.getTotalFiles()));
	}

	public static Response clientGetEventsOfTable(String table) throws IOException {
		return newFixedLengthResponse(Status.OK, NanoHTTPD.MIME_PLAINTEXT,
				String.valueOf(client.getEventsOfTable(table)));
	}

	public static Response clientGetFilesOfTable(String table) throws IOException {
		return newFixedLengthResponse(Status.OK, NanoHTTPD.MIME_PLAINTEXT,
				String.valueOf(client.getFilesOfTable(table)));
	}

	public static Response clientGetFileListOfTable(String table) throws IOException {
		StringBuffer sb = new StringBuffer();
		String[] files = client.getFileListOfTable(table);
		if (files.length > 0)
			sb.append(files[0]);
		for (int i = 1; i < files.length; i++)
			sb.append("," + files[i]);
		return newFixedLengthResponse(Status.OK, NanoHTTPD.MIME_PLAINTEXT, sb.toString());
	}

	public static Response clientGetWriteSpeed(String start, String end) throws IOException {
		StringBuffer sb = new StringBuffer();
		int[] speed = client.getWriteSpeed(Long.valueOf(start), Long.valueOf(end));
		if (speed.length > 0)
			sb.append(speed[0]);
		for (int i = 1; i < speed.length; i++)
			sb.append("," + speed[i]);
		return newFixedLengthResponse(Status.OK, NanoHTTPD.MIME_PLAINTEXT, sb.toString());
	}

	public static Response clientGetWriteSpeedOfLast(String seconds) throws IOException {
		StringBuffer sb = new StringBuffer();
		int[] speed = client.getWriteSpeed(Integer.valueOf(seconds));
		if (speed.length > 0)
			sb.append(speed[0]);
		for (int i = 1; i < speed.length; i++)
			sb.append("," + speed[i]);
		return newFixedLengthResponse(Status.OK, NanoHTTPD.MIME_PLAINTEXT, sb.toString());
	}

	public static Response clientGetReadLatency(String start, String end) throws IOException {
		StringBuffer sb = new StringBuffer();
		int[] speed = client.getReadLatency(Long.valueOf(start), Long.valueOf(end));
		if (speed.length > 0)
			sb.append(speed[0]);
		for (int i = 1; i < speed.length; i++)
			sb.append("," + speed[i]);
		return newFixedLengthResponse(Status.OK, NanoHTTPD.MIME_PLAINTEXT, sb.toString());
	}

	public static Response clientGetReadLatencyOfLast(String seconds) throws IOException {
		StringBuffer sb = new StringBuffer();
		int[] speed = client.getReadLatency(Integer.valueOf(seconds));
		if (speed.length > 0)
			sb.append(speed[0]);
		for (int i = 1; i < speed.length; i++)
			sb.append("," + speed[i]);
		return newFixedLengthResponse(Status.OK, NanoHTTPD.MIME_PLAINTEXT, sb.toString());
	}

	@Override
	public Response serve(IHTTPSession session) {
		try {
			String uri = session.getUri();
			Matcher heQuery = HE_QUERY.matcher(uri);
			Matcher staticSrc = STATIC_SRC.matcher(uri);

			if (uri.equals("") || uri.equals("/") || uri.equals("/index") || uri.equals("/index.html")) {
				return newChunkedResponse(Status.OK, NanoHTTPD.MIME_HTML, getResourceAsStream("html/index.html"));
			} else if (uri.equals("eventdb:info")) {
				Map<String, String> params = session.getParms();
				String op = params.get("op");
				if ("tableList".equals(op))
					return clientGetTableList();
				else if ("totalEvents".equals(op))
					return clientGetTotalEvents();
				else if ("totalFiles".equals(op))
					return clientGetTotalFiles();
				else if ("eventsOfTable".equals(op))
					return clientGetEventsOfTable(params.get("table"));
				else if ("filesOfTable".equals(op))
					return clientGetFilesOfTable(params.get("table"));
				else if ("fileListOfTable".equals(op))
					return clientGetFileListOfTable(params.get("table"));
				else if ("writeSpeed".equals(op))
					return clientGetWriteSpeed(params.get("startTimeStamp"), params.get("endTimeStamp"));
				else if ("writeSpeedOfLast".equals(op))
					return clientGetWriteSpeedOfLast(params.get("lastSeconds"));
				else if ("readLatency".equals(op))
					return clientGetReadLatency(params.get("startTimeStamp"), params.get("endTimeStamp"));
				else if ("readLatencyOfLast".equals(op))
					return clientGetReadLatencyOfLast(params.get("lastSeconds"));
				else
					return newFixedLengthResponse(Status.NOT_FOUND, NanoHTTPD.MIME_PLAINTEXT, "NOT FOUND");
			} else if (heQuery.matches()) {
				HeQueryClient client = new HeQueryClient(heQuery.group(1));
				List<byte[]> result = client.query(session.getQueryParameterString());
				List<HeEventDecoder.He> evts = HeEventDecoder.decode(result);
				StringBuilder sb = new StringBuilder();
				sb.append("time,detID,channel,pulse,eventType");
				for (HeEventDecoder.He he : evts)
					sb.append(String.format("\n%f,%d,%d,%d,%d", he.time, he.detID & 0x00ff, he.channel & 0x00ff,
							he.pulse & 0x00ff, he.eventType & 0x00ff));
				client.close();
				return newFixedLengthResponse(Status.OK, NanoHTTPD.MIME_PLAINTEXT, sb.toString());
			} else if (staticSrc.matches()) {
				String file = staticSrc.group(1);
				String[] mimeassist = file.split("\\.");
				String mime = MIME_TYPES.get(mimeassist[mimeassist.length - 1].trim());
				return newChunkedResponse(Status.OK, mime, getResourceAsStream(file));
			} else {
				return newFixedLengthResponse(Status.NOT_FOUND, NanoHTTPD.MIME_PLAINTEXT, "NOT FOUND");
			}
		} catch (Exception e) {
			return newFixedLengthResponse(Status.BAD_REQUEST, NanoHTTPD.MIME_PLAINTEXT, e.toString());
		}
	}
}