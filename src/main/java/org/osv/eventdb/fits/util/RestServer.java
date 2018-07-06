package org.osv.eventdb.fits.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
      new RestServer(port);
    } catch (IOException ioe) {
      System.err.println("Couldn't start server:\n" + ioe);
    }
  }

  public static Pattern HE_QUERY = Pattern.compile("/he/(\\w+)");
  public static Pattern STATIC_SRC = Pattern.compile("/static/(.+)");
  public static Map<String, String> MIME_TYPE = NanoHTTPD.mimeTypes();
  public static String EVENTDB_ENV = System.getenv("EVENTDB_ENV");

  public static InputStream getResourceAsStream(String path) throws IOException {
    if ("debug".equals(EVENTDB_ENV)) {
      String staticRoot = "src/main/resources/";
      return new FileInputStream(staticRoot + path);
    } else {
      return RestServer.class.getClassLoader().getResourceAsStream(path);
    }
  }

  @Override
  public Response serve(IHTTPSession session) {
    try {
      String uri = session.getUri();
      Matcher heQuery = HE_QUERY.matcher(uri);
      Matcher staticSrc = STATIC_SRC.matcher(uri);
      if (uri.equals("") || uri.equals("/") || uri.equals("/index") || uri.equals("/index.html")) {
        return newChunkedResponse(Status.OK, NanoHTTPD.MIME_HTML, getResourceAsStream("html/index.html"));
      } else if (heQuery.matches()) {
        HeQueryClient client = new HeQueryClient(heQuery.group(1));
        List<byte[]> result = client.query(session.getQueryParameterString());
        List<HeEventDecoder.He> evts = HeEventDecoder.decode(result);
        StringBuilder sb = new StringBuilder();
        sb.append("time,detID,channel,pulse,eventType");
        for (HeEventDecoder.He he : evts)
          sb.append(String.format("\n%f,%d,%d,%d,%d", he.time, he.detID & 0x00ff, he.channel & 0x00ff,
                  he.pulse & 0x00ff, he.eventType & 0x00ff));
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