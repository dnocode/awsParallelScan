package com.dnocode.utils.aws;


import org.joda.time.DateTime;
import org.joda.time.Seconds;
import java.util.ArrayList;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.List;
import java.util.concurrent.TimeUnit;
/**
 tic for respect the dynamo throughput limit
 */
public abstract   class TicThread<T> implements Runnable{

    protected final int interval=1;
    protected ScheduledExecutorService execTimer;
    protected final List<T> list;
    protected final int throughput;
    protected  int time=0;
    protected StatusListener listener;
    private int tollerance=100;
    DateTime startTime;
    int executionTime=0;
    int executionTimeLimit;
    protected final TimerTask timerTask=new TimerTask() {
        @Override
        public void run() {

             if(execTimer.isShutdown()){return;}
             boolean lastExecution;
             int startIndex=time*throughput;
             int endIndex=(lastExecution=startIndex+throughput>=list.size())?list.size():startIndex+throughput;
             onInterval(list.subList(startIndex,endIndex));
             time++;
             if(lastExecution){execTimer.shutdownNow(); }
        }
    };

    public TicThread(List<T> list, int throughput){
        this.list=list;
        this.throughput=throughput;
    }

    public TicThread(int throughput){
        this(new ArrayList(), throughput);
    }

    @Override
    public void run() {
        startTime=new DateTime();
        executionTimeLimit=list.size()/throughput+tollerance;
        beforeRun();
        execTimer= Executors.newScheduledThreadPool(4);
        execTimer.scheduleWithFixedDelay(timerTask, 0, interval, TimeUnit.SECONDS);
        while (!execTimer.isTerminated()){
            if((executionTime= Seconds.secondsBetween(startTime, new DateTime()).getSeconds())>executionTimeLimit){
              //  cronLog.error("error too much time");
                if(!execTimer.isShutdown()){execTimer.shutdownNow();}
            }
        }
        if(listener!=null){listener.terminated();}
    }

    public void beforeRun(){}

    public abstract  void onInterval(List<T> slice);

    public void setStatusListener(StatusListener listener){this.listener=listener;}

    public interface   StatusListener{ void terminated();}


}
