package org.osv.eventdb.util;

import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

public class Merge {
    public int[] value;
    public SortedMap<Integer, BitArray> bitmap = new TreeMap<Integer, BitArray>();
    public SortedMap<Integer, BitArray> topk = null;
    public SortedMap<Integer, BitArray> range = null;
    int[] pivot;
    private int len = 1024 * 1024 * 8;
    public int cardinal;
    public Merge(int c) {
        cardinal = c;
        value = new int[len];
        Random rdm = new Random();

        for(int k = 0; k < cardinal; k++) {
            BitArray ba = new BitArray(len);
            bitmap.put(k, ba);
        }

        for(int i = 0; i < len; i++){
            value[i] = rdm.nextInt(c);
            bitmap.get(value[i]).set(i);
        }
    }
    public void setTopk(int k) {
        if(topk != null){
            topk.clear();
            topk = null;
        }
        topk = new TreeMap<Integer, BitArray>();
        if(range != null) {
            range.clear();
            range = null;
        }
        range = new TreeMap<Integer, BitArray>();
        int step = cardinal / k;
        pivot = new int[k];
        for(int i = 0; i < k; i++){
            int temp = step * (i + 1);
            if(temp >= cardinal)
                temp = cardinal - 1;
            pivot[i] = temp;
        }
        for(int i = 0; i < k; i++) {
            topk.put(pivot[i], bitmap.get(pivot[i]));
            BitArray temp = new BitArray(len);
            if(i == 0) {
                for(int j = 0; j < pivot[i]; j++)
                    temp.or(bitmap.get(j));
            }else{
                for(int j = pivot[i - 1] + 1; j < pivot[i]; j++)
                    temp.or(bitmap.get(j));
            }
            range.put(pivot[i], temp);
        }
        if(pivot[k - 1] != cardinal - 1) {
            BitArray temp = new BitArray(len);
            for(int j = pivot[k - 1] + 1; j < cardinal - 1; j++)
                range.put(cardinal, temp);
        }
        double rouSum = 0;
        double cRatio = 0;
        int rouCnt = 0;
        for(BitArray ba: range.values()){
            BRLE brle = BRLE.compress(ba.getBits());
            rouSum += brle.getROU();
            cRatio += brle.getRatio();
            rouCnt++;
        }
        System.out.println("ROU=" + (rouSum/rouCnt) + "  CRatio=" + (cRatio/rouCnt));
    }
    public void equery(int k, boolean fromTopk){
        int cnt = 0;
        long startTime =  System.currentTimeMillis();

        if(!fromTopk) {
            BitArray result = bitmap.get(k);
            if(result != null){
                cnt = result.count();
            }
        }else{
            BitArray result = topk.get(k);
            if(result != null){
                for(int i = 0; i < len; i++)
                    if(result.get(i))
                        cnt++;
            }
            else{
                result = range.get(range.tailMap(k).firstKey());
                if(result != null){
                    for(int i = 0; i < len; i++)
                        if(result.get(i))
                            if(value[i] == k)
                                cnt++;
                }
            }
        }

        long endTime =  System.currentTimeMillis();
        System.out.printf("%dms 搜索到%d个结果\n", (endTime - startTime), cnt);
    }
    public void rquery(int s, int e, boolean fromTopk){
        int cnt = 0;
        long startTime =  System.currentTimeMillis();

        if(!fromTopk) {
            SortedMap<Integer, BitArray> result = bitmap.subMap(s, e);
            BitArray temp = new BitArray(len);
            for(BitArray ba: result.values())
                temp.or(ba);
            cnt = temp.count();
        }else{
            SortedMap<Integer, BitArray> result1 = topk.subMap(s, e);
            BitArray temp = new BitArray(len);
            for(BitArray ba: result1.values())
                temp.or(ba);
            SortedMap<Integer, BitArray> result2 = range.subMap(s, e);
            for(BitArray ba: result2.values())
                temp.or(ba);
            int last = result2.lastKey();
            for(int i = 0; i < pivot.length; i++){
                if(pivot[i] == last){
                    if((i+1) < pivot.length)
                        temp.or(range.get(pivot[i+1]));
                    if((i+2) < pivot.length)
                        temp.or(range.get(pivot[i+2]));
                }
            }
            //temp.or(range.get(range.tailMap(result2.lastKey()).firstKey()));
            for(int i = 0; i < len; i++)
                if(temp.get(i))
                    if(value[i] < e && value[i] >= s)
                        cnt++;
        }

        long endTime =  System.currentTimeMillis();
        System.out.printf("%dms 搜索到%d个结果\n", (endTime - startTime), cnt);
    }
}
