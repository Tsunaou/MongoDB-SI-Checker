package History.MongoDB;

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
        LogicalClock clock = new LogicalClock(1, 1);
        System.out.println(clock.getLongTime());
    }
}
