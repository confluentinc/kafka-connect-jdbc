package io.confluent.connect.jdbc.util;

public abstract class TaskSchedulerFactory {
    public static TaskScheduler create(String modeStr) {
        if (modeStr == null || modeStr.isEmpty()) {
            return new NativeTaskScheduler();
        }
        TaskScheduler.Mode mode = TaskScheduler.Mode.valueOf(modeStr);
        switch (mode) {
            case ONCE:
                return new NativeTaskScheduler();
            case SCHEDULER:
                return new PeriodTaskScheduler();
        }
        return new NativeTaskScheduler();
    }
}
