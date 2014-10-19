package edu.cs236.skyline;
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Map extends Mapper<LongWritable, Text, LongWritable, Gsod> {
    private LongWritable id = new LongWritable();
    public void map(LongWritable key, Text Gsod, Context context) throws IOException, InterruptedException {
        String weatherString = Gsod.toString();
        Gsod g_obj = new Gsod();
        int Partition = Integer.parseInt(context.getConfiguration().get("Partition"));
        List<String> values = new ArrayList<String>(Arrays.asList(weatherString.split("\\|")));
        List<String> theKeys = Arrays.asList(values.get(0).split("\\s+"));
        g_obj.setKey(Long.parseLong(theKeys.get(1)));
        g_obj.setStation(values.get(1));
        g_obj.setYear(values.get(2));
        g_obj.setModa(values.get(3));
        g_obj.setTemp(values.get(4));       
        g_obj.setDewp(values.get(5));
        g_obj.setSlp(values.get(6));
        g_obj.setMax(values.get(8));
        g_obj.setStp(values.get(7));
        g_obj.setWdsp(values.get(9));
        g_obj.setMxspd(values.get(10));
        g_obj.setGust(values.get(11));
        g_obj.setMin(values.get(12));
        id.set(w.getKey() % Partition);
        context.write(id, w);
    }
}
