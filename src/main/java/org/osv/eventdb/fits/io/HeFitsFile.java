package org.osv.eventdb.fits.io;

import org.osv.eventdb.fits.evt.*;

import java.io.*;

public class HeFitsFile extends FitsFile<HeEvt>{
	public HeFitsFile(String filepath){
		super(filepath, 2880 * 2 + 80 * 4 + 9, 2880 * 4, 16);
	}
	protected HeEvt getEvt(byte[] evtBin){
		try{
			return new HeEvt(evtBin);
		}catch(IOException e){
			e.printStackTrace();
		}
		return null;
	}
}
