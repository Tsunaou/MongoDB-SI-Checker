package History.MongoDB;


import java.util.Comparator;

public class LogicalClock implements Comparable<LogicalClock>{
    public long time;
    public long inc;

    public LogicalClock(long time, long inc) {
        this.time = time;
        this.inc = inc;
    }

    @Override
    public String toString() {
        return "(" + time + "," + inc + ')';
    }

    @Override
    public int compareTo(LogicalClock clock) {
        if(this.time != clock.time){
            return Long.compare(this.time, clock.time);
        }else{
            return Long.compare(this.inc, clock.inc);
        }
    }

    public static void main(String[] args) {
        LogicalClock t1 = new LogicalClock(1636007504,8);
        LogicalClock t2 = new LogicalClock(1636007490,25);
        System.out.println(t1);
        System.out.println(t2);
        System.out.println(t1.compareTo(t1) == 0);
    }
}
