package edu.cs236.skyline;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;


public class LastReduce extends Reducer<LongWritable, Gsod, Text, Gsod> {
	private Text one = new Text();
    public void reduce(LongWritable key, Iterable<Gsod> Gsod, Context context)
            throws IOException, InterruptedException {
        java.util.Map<Long, Gsod> skylineMap = new ConcurrentHashMap<Long, Gsod>();
        int i = 0;
        for (Gsod weathers : Gsod) {
            Gsod wOuter = new Gsod();
            wOuter.copyObject(weathers);
            if (skylineMap.isEmpty()) {
                skylineMap.put(wOuter.getKey(), wOuter);
            } else {
                boolean addToSkyline = false;
                for (java.util.Map.Entry<Long, Gsod> wInner : skylineMap.entrySet()) {
                    int skyline = 0;
                    int weather = 0;
					int maxMxspd = min(wOuter.getMxspd(), wInner.getValue().getMxspd());
                    int minGust = min(wOuter.getGust(), wInner.getValue().getGust());
                    int maxTemp = max(wOuter.getTemp(), wInner.getValue().getTemp());
                    int maxDewp = max(wOuter.getDewp(), wInner.getValue().getDewp());
                    int maxSlp = max(wOuter.getSlp(), wInner.getValue().getSlp());
                    int minStp = min(wOuter.getStp(), wInner.getValue().getStp());
                    int minWdsp = min(wOuter.getWdsp(), wInner.getValue().getWdsp());
                    int maxMxspd = min(wOuter.getMxspd(), wInner.getValue().getMxspd());
                    int minGust = min(wOuter.getGust(), wInner.getValue().getGust());
                    int maxMax = min(wOuter.getMax(), wInner.getValue().getMax());
                    int minMin = min(wOuter.getMin(), wInner.getValue().getMin());

                    if (maxTemp > 0) {
                        weather++;
                    } else if (maxTemp < 0) {
                        skyline++;
                    } 
                    if (maxDewp > 0) {
                        weather++;
                    } else if (maxDewp < 0) {
                        skyline++;
                    } 
                    if (maxSlp > 0) {
                        weather++;
                    } else if (maxSlp < 0) {
                        skyline++;
                    } 
                    if (minStp > 0) {
                        weather++;
                    } else if (minStp < 0) {
                        skyline++;
                    } 
                    if (minWdsp > 0) {
                        weather++;
                    } else if (minWdsp < 0) {
                        skyline++;
                    } 
                    if (maxMxspd > 0) {
                        weather++;
                    } else if (maxMxspd < 0) {
                        skyline++;
                    } 
                    if (minGust > 0) {
                        weather++;
                    } else if (minGust < 0) {
                        skyline++;
                    } 

                    if (maxMax > 0) {
                        weather++;
                    } else if (maxMax < 0) {
                        skyline++;
                    } 
                    if (minMin > 0) {
                        weather++;
                    } else if (minMin < 0) {
                        skyline++;
                    } 
                    if (skyline == 0 && weather > 0) {
                 
                        skylineMap.remove(wInner.getKey());
                        addToSkyline = true;
                        i--;
                        if (weather == 1) break;
                    }
                 
                    else if (weather == 0) {
                        addToSkyline = false;
                 
                        break;
                    } else {
                        addToSkyline = true;
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
        for (java.util.Map.Entry<Long, Gsod> w : skylineMap.entrySet()) {
            String id = w.getValue().getStation() + "_" + w.getValue().getYear() + "_" + w.getValue().getModa();
            one.set(id);
            context.write(one, w.getValue());
        }
    }


}
