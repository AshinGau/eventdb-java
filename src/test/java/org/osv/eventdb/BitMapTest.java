package org.osv.eventdb;

import org.junit.Test;
import org.osv.eventdb.util.BRLE;
import org.osv.eventdb.util.BitArray;
import java.util.Random;

import org.osv.eventdb.util.Cunit;
import org.osv.eventdb.util.Merge;
import org.xerial.snappy.Snappy;

public class BitMapTest {
	@Test
	public void testOpSpeed() throws Exception {
		int len = 1024 * 1024 * 8;
		int setLen = len / 100;
		BitArray b1 = new BitArray(len);
		BitArray b2 = new BitArray(len);
		Random rdm = new Random();
		for(int i = 0; i < setLen; i++) {
			b1.set(rdm.nextInt(len));
			b2.set(rdm.nextInt(len));
		}
		long startTime =  System.currentTimeMillis();
		for(int i = 0; i < 1024; i++)b1.or(b2);
		long endTime =  System.currentTimeMillis();
		System.out.println("1GB bitmap time cost: " + (endTime - startTime) + "ms");

	}
	@Test
	public void testBRLE() throws Exception {
		int len = 1024 * 1024 * 8;
		int setLen = len / 1000;
		BitArray b1 = new BitArray(len);
		BitArray b2 = new BitArray(len);
		Random rdm = new Random();
		for(int i = 0; i < setLen; i++) {
			b1.set(rdm.nextInt(len));
			b2.set(rdm.nextInt(len));
		}
		BRLE bc1 = BRLE.compress(b1.getBits());
		BRLE bc2 = BRLE.compress(b2.getBits());
		System.out.printf("ROU: %f, Ratio: %f\n", bc1.getROU(), bc1.getRatio());
		System.out.printf("ROU: %f, Ratio: %f\n", bc2.getROU(), bc2.getRatio());
		long startTime =  System.currentTimeMillis();
		BRLE result = bc1.or(bc2);
		long endTime =  System.currentTimeMillis();
		System.out.println("1GB bitmap time cost: " + (endTime - startTime) + "ms");
	}

	@Test
	public void testMerge() throws Exception {
		Merge m = new Merge(1000);
		assistMerge(m, 5);
		assistMerge(m, 10);
		assistMerge(m, 20);
		assistMerge(m, 30);
		assistMerge(m, 40);
		assistMerge(m, 50);
		assistMerge(m, 60);
		assistMerge(m, 70);
		assistMerge(m, 80);
		assistMerge(m, 90);
		assistMerge(m, 100);
		assistMerge(m, 200);
		assistMerge(m, 300);
		assistMerge(m, 400);
	}
	private static void assistMerge(Merge m, int k) {
		System.out.printf("\n###top-%d merge result###\n", k);
		m.setTopk(k);
		m.equery(16, false);
		m.equery(16, true);
		m.rquery(16, 329, false);
		m.rquery(16, 329, true);
	}
}
