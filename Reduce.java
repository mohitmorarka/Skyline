package edu.cs236.skyline;
import org.apache.hadoop.io.LongWritable;
import java.util.Map;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<LongWritable, Gsod, LongWritable, Gsod> {
    private LongWritable one = new LongWritable();
    public void reduce(LongWritable key, Iterable<Gsod> Gsod, Context context)
            throws IOException, InterruptedException {
        Map<Long, Gsod> skylineMap = new ConcurrentHashMap<Long, Gsod>();
        int i = 0;
        for (Gsod weathers : Gsod) {
            Gsod wOuter = new Gsod();
            wOuter.copyObject(weathers);
            if (skylineMap.isEmpty()) {
                skylineMap.put(wOuter.getKey(), wOuter);
            } else {
                boolean addToSkyline = false;
                for (Map.Entry<Long, Gsod> wInner : skylineMap.entrySet()) {
                    int skyline = 0,int weather = 0,int eq = 0;
					int Stp = min(wOuter.getStp(), wInner.getValue().getStp());
                    int Temp = max(wOuter.getTemp(), wInner.getValue().getTemp());                  
                    int Stp = min(wOuter.getStp(), wInner.getValue().getStp());
                    if (Temp > 0) {
                        weather++;
                    } else if (Temp < 0) {
                        skyline++;
                    }  
                    if (Stp > 0) {
                        weather++;
                    } else if (Stp < 0) {
                        skyline++;
                    } 
                    if (skyline == 0 && weather > 0) {
                        skylineMap.remove(wInner.getKey());
                        addToSkyline = true;
                        i--;                        
                    }                  
                }
                if (addToSkyline) {
                    skylineMap.put(wOuter.getKey(), wOuter);
                    i++;
                }
            }
        }
	public static int min(double weather, double skyline) {
        if (weather < skyline) {
            return 1;
        } else if (weather > skyline) {
            return -1;
        } else {
            return 0;
        }
    }
	public static int max(double weather, double skyline) {
        if (weather > skyline) {
            return 1;
        } else if (weather < skyline) {
            return -1;
        } else {
            return 0;
        }
    }
        for (Map.Entry<Long, Gsod> w : skylineMap.entrySet()) {
            one.set(w.getKey());
            context.write(one, w.getValue());
        }
    }
}