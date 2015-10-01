package com.dnocode.utils.aws;


import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;

/**
 * Created by dino on 31/07/15.
 */
public class AmazonConsumerThreadPool<T> {

    private final LinkedTransferQueue<T> sharedItemsList;
    private final BlockingQueue<T> chunk;
    private final ExecutorService threadPool;
    private final LinkedBlockingQueue<Runnable> uploaderThreadQueue = new LinkedBlockingQueue<>();
    private ChunkProcessor[] chunkProcessors;
    /**
     * waiting time before giving the shutdown to threadpool
     **/
    private int keepAliveTime = 500;
    private int timeFromDie = keepAliveTime;
    private final InternalThreadConsumer internalThreadConsumer = new InternalThreadConsumer();

    public AmazonConsumerThreadPool(LinkedTransferQueue scannedItems, int chunksize, ChunkProcessor... chunkProcessors) {

        this.sharedItemsList = scannedItems;
        this.chunk = new ArrayBlockingQueue(chunksize);
        this.threadPool = new ThreadPoolExecutor(20, 50, 120, TimeUnit.SECONDS, uploaderThreadQueue);
        this.chunkProcessors = chunkProcessors;
    }

    ExecutorService getThreadPool() {
        return threadPool;
    }

    public void listen() {
        new Thread(internalThreadConsumer).start();
    }

    /**
     * this thread listen for element comes from
     * scanner and when the chunk limit is rich
     * then create and submit chhunk processor thread
     */
    private class InternalThreadConsumer implements Runnable {

        @Override
        public void run() {
            try {
                Timer timer = new Timer();
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        timeFromDie--;
                        if (Math.abs(timeFromDie) > keepAliveTime && threadPool.isShutdown() == false) {
                            submitChunkToProcessors();
                            threadPool.shutdown();
                        }
                    }
                }, 0, 1);

                while (timeFromDie > 0) {
                    /**wait for scanned items**/
                    T item = sharedItemsList.take();
                    timeFromDie = keepAliveTime;
                    chunk.add(item);
                    /**if chunk size is reached start uploader task**/
                    if (chunk.remainingCapacity() == 0) {
                        submitChunkToProcessors();
                    }
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * submit task to the
     * threadpool
     */
    protected void submitChunkToProcessors() {

        ArrayList<T> deliveringChunk = new ArrayList<>();
        this.chunk.drainTo(deliveringChunk);

        if (deliveringChunk.size() > 0) {
            for (ChunkProcessor processor : chunkProcessors) {
                try {
                    ChunkProcessor clone = processor.clone();
                    clone.initChunk(deliveringChunk);
                    threadPool.submit(clone);

                } catch (CloneNotSupportedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * chunk processor
     * its runnable that elaborate
     * chunk when is ready
     */
    public static abstract class ChunkProcessor implements Runnable, Cloneable {

        protected List chunk;

        public ChunkProcessor() {
        }

        public void initChunk(List chunk) {
            this.chunk = chunk;
        }

        public abstract void processChunk();

        @Override
        public void run() {
            processChunk();
        }

        protected ChunkProcessor clone() throws CloneNotSupportedException {
            ChunkProcessor clone = (ChunkProcessor) super.clone();
            return clone;
        }
    }
}
