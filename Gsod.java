package edu.cs236.skyline;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Gsod implements Writable {
    private long key;
    private int station;
    private int wban;
    private int year; 
    private int moda;
    private double temp; 
    private double countTemp;
    private double dewp; 
    private double countDewp;
    private double slp; 
    private int countSlp;
    private double stp; 
    private int countStp;
    private double visib;
    private int countVisib;
    private double wdsp; 
    private int countWdsp;
    private double mxspd;
    private double gust; 
    private double max;
    private char flagMaxTemp;
    private double min; 
    private char flagMin;
    private double prcp;
    private char flagPrcp;
    private double sndp;
    private int frshtt;

    public Gsod() {
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.key).append("|").append(this.station).append("|").append(this.year).append("|");
        sb.append(this.moda).append("|").append(this.temp).append("|").append(this.dewp).append("|");
        sb.append(this.slp).append("|").append(this.stp).append("|").append(this.max).append("|");
        sb.append(this.wdsp).append("|").append(this.mxspd).append("|").append(this.gust).append("|");
        sb.append(this.min);

        return sb.toString();
    }
	
	public void setKey(long k) {
        this.key = k;
    }

    public void setStation(String m) {
        this.station = Integer.parseInt(m);
    }

    public void setYear(String m) {
        this.year = Integer.parseInt(m);
    }

    public void setModa(String m) {
        this.moda = Integer.parseInt(m);
    }

    public void setTemp(String m) {
        this.temp = Double.parseDouble(m);
    }

    public void setCountTemp(String m) {
        this.countTemp = Double.parseDouble(m);
    }

    public void setDewp(String m) {
        this.dewp = Double.parseDouble(m);
    }

    public void setCountDewp(String m) {
        this.countDewp = Double.parseDouble(m);
    }

    public void setSlp(String m) {
        this.slp = Double.parseDouble(m);
    }

    public void setCountSlp(String m) {
        this.countSlp = Integer.parseInt(m);
    }

    public void setStp(String m) {
        this.stp = Double.parseDouble(m);
    }

    public void setCountStp(String m) {
        this.countStp = Integer.parseInt(m);
    }

    public void setVisib(String m) {
        this.visib = Double.parseDouble(m);
    }

    public void setCountVisib(String m) {
        this.countVisib = Integer.parseInt(m);
    }

    public void setWdsp(String m) {
        this.wdsp = Double.parseDouble(m);
    }

    public void setCountWdsp(String m) {
        this.countWdsp = Integer.parseInt(m);
    }

    public void setMxspd(String m) {
        this.mxspd = Double.parseDouble(m);
    }

    public void setMax(String m) {
        this.max = Double.parseDouble(m);
    }

    public void setMin(String m) {
        this.min = Double.parseDouble(m);
    }

    public void setGust(String m) {
        this.gust = Double.parseDouble(m);
    }
    public long getKey() {
        return this.key;
    }

    public int getStation() {
        return this.station;
    }

    public int getYear() {
        return this.year;
    }

    public int getModa() {
        return this.moda;
    }

    public double getTemp() {
        return this.temp;
    }

    public double getDewp() {
        return this.dewp;
    }

    public double getSlp() {
        return this.slp;
    }

    public double getMax() {
        return this.max;
    }

    public double getStp() {
        return this.stp;
    }

    public double getWdsp() {
        return this.wdsp;
    }

    public double getMxspd() {
        return this.mxspd;
    }

    public double getGust() {
        return this.gust;
    }

    public double getMin() {
        return this.min;
    }

    public static Gsod read(DataInput in) throws IOException {
        Gsod w = new Gsod();
        w.readFields(in);
        return w;
    }

    
    public void readFields(DataInput in) throws IOException {
        //
    }

    public void write(DataOutput out) throws IOException {
        //
    }

    public void copyObject(Gsod in) {
        this.key = in.getKey();
        this.station = in.getStation();
        this.year = in.getYear();
        this.moda = in.getModa();
        this.temp = in.getTemp();
        this.dewp = in.getDewp();
        this.slp = in.getSlp();
        this.max = in.getMax();
        this.stp = in.getStp();
        this.wdsp = in.getWdsp();
        this.mxspd = in.getMxspd();
        this.gust = in.getGust();
        this.min = in.getMin();
    }

    

	

}
