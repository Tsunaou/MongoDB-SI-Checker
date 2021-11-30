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

    public LogicalClock getAdvance(LogicalClock clock){
        if(this.compareTo(clock) < 0){
            return null;
        }else{
            long incDiff = this.inc - clock.inc;
            if(incDiff >= 0){
                return new LogicalClock(this.time - clock.time, incDiff);
            }else{
                return new LogicalClock(this.time -1 - clock.time, incDiff + Long.MAX_VALUE);
            }
        }

    }

    public String toSecond(){
        return String.valueOf((double) this.time / 1000000000);
    }

    public static void main(String[] args) {
        LogicalClock t1 = new LogicalClock(1636007504,8);
        LogicalClock t2 = new LogicalClock(1636007490,25);
        System.out.println(t1);
        System.out.println(t2);
        System.out.println(t1.compareTo(t1) == 0);
        System.out.println(t1.getAdvance(t2));
    }




}
