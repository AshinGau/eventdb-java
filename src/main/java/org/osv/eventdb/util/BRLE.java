package org.osv.eventdb.util;

import java.util.LinkedList;
import java.util.List;

public class BRLE {
    public List<Cunit> units;
    public int ori = 0;
    public int comp = 0;
    public int uncompLen = 0;
    public byte[] unCompressedBytes;
    public double getRatio() {
        return (double)((ori + comp)*4) / (double)uncompLen;
    }
    public double getROU() {
        return (double)ori/(double)(ori + comp);
    }
    private BRLE(byte[] bin, boolean doComp) {
        if(doComp)
            this.doComp(bin);
    }
    public BRLE(){
        units = new LinkedList<Cunit>();
        unCompressedBytes = null;
    }
    public static BRLE compress(byte[] bin) {
        return new BRLE(bin, true);
    }
    public static BRLE uncompress(byte[] bin) {
        return new BRLE(bin, false);
    }
    private void doComp(byte[] bin) {
        unCompressedBytes = bin;
        uncompLen = bin.length;
        BitArray ba = new BitArray(bin);
        units = new LinkedList<Cunit>();
        int bitLen = ba.getBitLength();
        int pos = 0;
        for(int i = 0; i < bitLen; i++) {
            int cnt = 0;
            boolean fill = ba.get(i);
            for(; i < bitLen; i++) {
                if(ba.get(i) == fill)
                    cnt++;
                else
                    break;
            }
            if(cnt > 31) {//compression unit
                Cunit u = new Cunit();
                u.setU0(true);
                u.setU1(fill);
                u.setRunLen(cnt);
                pos = i;
                i--;
                units.add(u);
                comp++;
            } else {
                Cunit u = new Cunit();
                u.setU0(false);
                for(int j = 1; j < 32; j++)
                    u.unit.set(j, ba.get(pos++));
                i = pos - 1;
                units.add(u);
                ori++;
            }
        }
    }
    public BRLE or(BRLE bc) {
        LinkedList<Cunit> A = new LinkedList<Cunit>(this.units);
        LinkedList<Cunit> B = new LinkedList<Cunit>(bc.units);
        BRLE result = new BRLE();
        while(A.size() != 0 || B.size() != 0) {
            if(A.size() == 0) {
                for (Cunit c : B)
                    result.units.add(c);
                break;
            }else if(B.size() == 0) {
                for(Cunit c: A)
                    result.units.add(c);
                break;
            }else{
                Cunit ca = A.poll();
                Cunit cb = B.poll();
                if(ca.getU0() && cb.getU0()) {//both compressed
                    int la = ca.getRunLen();
                    int lb = cb.getRunLen();
                    Cunit c = new Cunit();
                    c.setU0(true);
                    if(ca.getU1() ^ cb.getU1()) {
                        c.setU1(true);
                    }else{
                        c.setU1(ca.getU1());
                    }
                    c.setRunLen(Math.min(la, lb));
                    Cunit temp = new Cunit();
                    temp.setU0(true);
                    if(la > lb) {
                        temp.setU1(ca.getU1());
                        temp.setRunLen(la - lb);
                        A.addFirst(temp);
                    }else if(la < lb) {
                        temp.setU1(cb.getU1());
                        temp.setRunLen(lb - la);
                        B.addFirst(temp);
                    }
                    result.units.add(c);
                }
                if(!ca.getU0() && !cb.getU0()) {//both uncompressed
                    Cunit c = new Cunit();
                    byte[] ba = ca.getBits();
                    byte[] bb = cb.getBits();
                    byte[] temp = new byte[4];
                    for(int i = 0; i < 4; i++)
                        temp[i] = (byte)(ba[i] | bb[i]);
                    c.unit = new BitArray(temp);
                    result.units.add(c);
                }
                if(ca.getU0() ^ cb.getU0()) {
                    Cunit c = new Cunit();
                    Cunit temp = new Cunit();
                    temp.setU0(true);
                    if(ca.getU0()) {
                        if(!ca.getU1()) {
                            c.unit = cb.unit;
                            temp.setU1(false);
                            temp.setRunLen(ca.getRunLen() - 31);
                        }
                        if(ca.getU1()) {
                            c.setU0(true);
                            c.setU1(true);
                            c.setRunLen(31);
                            temp.setU1(true);
                            temp.setRunLen(ca.getRunLen() - 31);
                        }
                        A.addFirst(temp);
                    }
                    else if(cb.getU0()) {
                        if(!cb.getU1()) {
                            c.unit = ca.unit;
                            temp.setU1(false);
                            temp.setRunLen(cb.getRunLen() - 31);
                        }
                        if(cb.getU1()) {
                            c.setU0(true);
                            c.setU1(true);
                            c.setRunLen(31);
                            temp.setU1(true);
                            temp.setRunLen(cb.getRunLen() - 31);
                        }
                        B.addFirst(temp);
                    }
                    result.units.add(c);
                }
            }
        }
        return result;
    }
}
