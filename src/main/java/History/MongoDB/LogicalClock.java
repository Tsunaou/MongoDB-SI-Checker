package History.MongoDB;


import java.math.BigInteger;

public class LogicalClock {
    public int time;
    public int inc;

    public LogicalClock(int time, int inc) {
        this.time = time;
        this.inc = inc;
    }

    public long getLongTime() {
        return (long) time << 32 + inc;
    }

    @Override
    public String toString() {
        return "(" + time + "," + inc + ')';
    }

    public static void main(String[] args) {
        LogicalClock t1 = new LogicalClock(1636007504,8);
        LogicalClock t2 = new LogicalClock(1636007490,25);
        System.out.println(t1.getLongTime());
        System.out.println(t2.getLongTime());
    }
}
