package org.osv.eventdb.util;

public class Cunit {
    public BitArray unit;
    public boolean u0;
    public boolean u1;
    public int runLen;
    public static int byteArrayToInt(byte[] b) {
        return   b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }
    public static byte[] intToByteArray(int a) {
        return new byte[] {
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
    }
    public Cunit(){
        unit = new BitArray(new byte[4]);
    }
    public void setU0(boolean bit) {
        u0 = bit;
        if(bit)
            unit.set(0);
        else
            unit.clear(0);
    }
    public boolean getU0() {
        return unit.get(0);
    }
    public boolean getU1() {
        return unit.get(1);
    }
    public void setU1(boolean bit) {
        u1 = bit;
        if(bit)
            unit.set(1);
        else
            unit.clear(1);
    }
    public void setRunLen(int len) {
        runLen = len;
        unit.or(intToByteArray(len));
    }
    public int getRunLen() {
        byte[] bin = unit.getBits();
        byte[] temp = new byte[4];
        for(int i = 0; i < 4; i++)
            temp[i] = bin[i];
        temp[0] &= 0x3F;
        return byteArrayToInt(temp);
    }
    public byte[] getBits() {
        return unit.getBits();
    }
}
