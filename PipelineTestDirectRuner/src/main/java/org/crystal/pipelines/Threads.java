package org.crystal.pipelines;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Threads {
    public static void main(String... args) {
        // TODO document why this method is empty
        for (int i = 0; i < 10000; i++) {
            runThread(i);
        }

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        System.out.println(Objects.requireNonNull(new File("").list()).length);

    }

    public static void runThread(int i) {
        new Thread(() -> {
            synchronized (Thread.class){
                System.out.println("thread runs" + i) ;

            }
        }).start();
    }


}
