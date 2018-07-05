package org.osv.eventdb.fits.util;

import java.io.IOException;
import java.util.List;

import org.osv.eventdb.fits.HeQueryClient;

import fi.iki.elonen.NanoHTTPD;
import fi.iki.elonen.NanoHTTPD.Response.Status;

public class RestServer extends NanoHTTPD {
	public RestServer(int port) throws IOException {
		super(port);
		start(NanoHTTPD.SOCKET_READ_TIMEOUT, false);
		System.out.println("RestFul Server is running at http://localhost:8081/ \n");
	}

	public static void runAtPort(int port) {
		try {
			new RestServer(port);
		} catch (IOException ioe) {
			System.err.println("Couldn't start server:\n" + ioe);
		}
	}

	@Override
	public Response serve(IHTTPSession session) {
		String[] uris = session.getUri().split("/");
		if (uris.length != 3) {
			return newFixedLengthResponse(Status.NOT_FOUND, NanoHTTPD.MIME_PLAINTEXT, "NOT FOUND");
		}
		if ("he".equals(uris[1])) {
			try {
				HeQueryClient client = new HeQueryClient(uris[2]);
				List<byte[]> result = client.query(session.getQueryParameterString());
				List<HeEventDecoder.He> evts = HeEventDecoder.decode(result);
				StringBuilder sb = new StringBuilder();
				sb.append("time,detID,channel,pulse,eventType");
				for (HeEventDecoder.He he : evts)
					sb.append(String.format("\n%f\t%d\t%d\t%d\t%d", he.time, he.detID & 0x00ff, he.channel & 0x00ff,
							he.pulse & 0x00ff, he.eventType & 0x00ff));
				return newFixedLengthResponse(Status.OK, NanoHTTPD.MIME_PLAINTEXT, sb.toString());

			} catch (Exception e) {
				return newFixedLengthResponse(Status.BAD_REQUEST, NanoHTTPD.MIME_PLAINTEXT, e.toString());
			}
		} else {
			return newFixedLengthResponse(Status.NOT_FOUND, NanoHTTPD.MIME_PLAINTEXT, "NOT FOUND");
		}
	}
}