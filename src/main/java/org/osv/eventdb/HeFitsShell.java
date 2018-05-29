package org.osv.eventdb;

import java.util.List;
import java.util.Scanner;

import org.osv.eventdb.event.PropertyValue;
import org.osv.eventdb.fits.BytePropertyValue;
import org.osv.eventdb.fits.FitsEvent;
import org.osv.eventdb.fits.HeEvent;
import org.osv.eventdb.fits.HeQueryClient;

public class HeFitsShell {
	public static void printEvents(List<byte[]> result) {
		for (byte[] events : result) {
			byte[] evtBin = new byte[16];
			int count = events.length / 16;
			for (int i = 0; i < count; i++) {
				int index = i * 16;
				for (int j = 0; j < 16; j++)
					evtBin[j] = events[index + j];
				FitsEvent event = new HeEvent(evtBin);
				PropertyValue[] values = event.getPropertyValues();
				System.out.printf("%f\t%d\t%d\t%d\t%d\n", event.getTime(),
						((BytePropertyValue) values[0]).getValue() & 0x00ff,
						((BytePropertyValue) values[1]).getValue() & 0x00ff,
						((BytePropertyValue) values[2]).getValue() & 0x00ff,
						((BytePropertyValue) values[3]).getValue() & 0x00ff);
			}
		}
		System.out.printf("time\t\t\tdetID\tchannel\tpulse\teventType\n");
	}

	public static void console(String tableName) {
		try {
			HeQueryClient client = new HeQueryClient(tableName);
			Scanner sc = new Scanner(System.in);
			while (true) {
				if (sc.hasNextLine()) {
					String command = sc.nextLine().trim();
					if (command.equals("quit"))
						return;
					long startTime = System.currentTimeMillis();
					List<byte[]> result = client.query(command);
					long endTime = System.currentTimeMillis();
					int count = 0;
					for (byte[] events : result)
						count += events.length / 16;
					printEvents(result);

					System.out.printf("Retrieve %d events in %.3fs\n\n", count, (endTime - startTime) / 1000.0);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}