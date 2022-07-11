package ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import WPQ.WaitablePriorityQueueCond;

public class ThreadPoolIMP implements Executor {
    public enum Priority {
        LOW,
        MEDIUM,
        HIGH;
    }
    /* initialized in Ctor */
    private List<ThreadImp> threads;
    private int numOfThreads;
    private WaitablePriorityQueueCond<Task<?>> tasks;
    /*priority*/
    private final int DEFAULT_PRIORITY = Priority.MEDIUM.ordinal();
    private final int HIGH_PROIORITY = (Priority.HIGH.ordinal())*10;
    private final int LOW_PROIORITY = (Priority.LOW.ordinal())-10;
    /* locks/semaphore */
    private Semaphore sem = new Semaphore(0);
    private Semaphore semPause = null;
    /* flags */
    private boolean isShutDown = false;
    private boolean isStopSubmit = false;
    private Callable<Void> killTask = () -> { Thread.currentThread().interrupt(); return null; };

    /* Ctor  */
    public ThreadPoolIMP(int numOfThreads){
        if(numOfThreads <= 0){
            throw new IllegalArgumentException("nThreads must be up to 0");
        }

        threads = new ArrayList<>();
        this.numOfThreads = numOfThreads;

        tasks = new WaitablePriorityQueueCond<>();

        for(int i = 0; i < numOfThreads; ++i){
            threads.add(new ThreadImp());
        }
        threads.forEach(t -> t.thread.start());

    }

    @Override
    public void execute(Runnable command) {
        submit(command);
    }

    public <T> Future<T> submit(Callable<T> callable, Priority priority){
        return submitImp(callable, priority.ordinal());
    }
    public <T> Future<T> submit(Callable<T> callable){
        return submitImp(callable, DEFAULT_PRIORITY);
    }

    public Future<Void> submit(Runnable runnable, Priority priority){
        return submitImp(Executors.callable(runnable,null), priority.ordinal());
    }
    public Future<Void> submit(Runnable runnable){
        return submitImp(Executors.callable(runnable,null), DEFAULT_PRIORITY);
    }

    public <T> Future<T> submit(Runnable runnable, Priority priority, T result){
        return submitImp(Executors.callable(runnable,result), priority.ordinal());
    }

    private <T> Future<T> submitImp(Callable<T> callable, int priority){
        Objects.requireNonNull(callable);
        Future<T> retVal = null;

        if(!isStopSubmit){
            Task<T> task = new Task<>(callable, priority);
            tasks.enqueue(task);
            retVal =  task.getFuture();
        }

        return retVal;
    }

    private boolean removeTask(Task<?> task){
        return tasks.remove(task);
    }
    public int getNumberOfThreads(){
        return numOfThreads;
    }

    public void setNumberOfThread(int newNumOfThreads){
        if(newNumOfThreads <= 0){
            throw new IllegalArgumentException("nThreads must be up to 0");
        }

        int diff = newNumOfThreads-numOfThreads;

        if(diff > 0){//add threads
            for(int i = 0; i < diff; ++i){
                ThreadImp newThread = new ThreadImp();
                threads.add(newThread);
                newThread.thread.start();
            }
        }
        else{//reduction threads
            diff *= (-1);
            for(int i = 0; i < diff; ++i){
                submitImp(killTask, HIGH_PROIORITY);
            }
        }

        numOfThreads = newNumOfThreads;
    }
    public void pause(){
        semPause = new Semaphore(0);
        Callable<Void> pauseTask = () -> {
            semPause.acquire();
            return null;
        };

        for(int i = 0; i < numOfThreads; ++i){
            submitImp(pauseTask, HIGH_PROIORITY);
        }
    }
    public void resume(){
        for(int i = 0; i < numOfThreads; ++i){
            semPause.release();
        }
    }

    public void shutdown(){
        isShutDown = true;
        for(int i = 0; i < numOfThreads; ++i){
            submitImp(killTask, LOW_PROIORITY);
        }
        isStopSubmit = true;
    }

    public void awaitTermination(){
        awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
    }

    public boolean awaitTermination(long timeout, TimeUnit unit){
        boolean ret = false;
        try {
            ret = sem.tryAcquire(numOfThreads,timeout,unit);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        isShutDown = false;
        System.out.println("done");
        return ret;
    }

    private class ThreadImp implements Runnable{
        private Thread thread = new Thread(this);
        private Task<?> currTask;

        @Override
        public void run() {
            while(!Thread.interrupted()){
                currTask = tasks.dequeue();
                //link thread to task
                currTask.setThread(thread);
                //link task to thread
                currTask.runTask();
            }
            threads.remove(this);

            if(isShutDown){
                sem.release();
            }
        }
    }

    private class Task<T> implements Comparable<Task<T>>{
        private final Callable<T> task;
        private final int priority;
        private final TaskFuture<T> taskFuture;
        private T retVal = null; // return value of the task
        private boolean isCanclled = false;
        private boolean isDone = false;
        private Thread currThread = null;
        private Exception executException = null;
        private ReentrantLock lock = new ReentrantLock();
        private Condition waitUntilDone = lock.newCondition();

        public Task(Callable<T> callable, int priority){
            task = callable;
            this.priority = priority;
            taskFuture = new TaskFuture<>();
        }

        public void runTask(){
            try {
                retVal = task.call();//the run func of callable
                isDone = true;
            } catch (Exception e) {
                e.printStackTrace();
                executException = e;
            }

            lock.lock();
            waitUntilDone.signal();
            lock.unlock();
        }

        public Future<T> getFuture(){//return the object Future
            return taskFuture;
        }

        public void setThread(Thread newThread){
            currThread = newThread;
        }

        @Override
        public int compareTo(Task<T> other) {
            return (other.priority-this.priority);
        }

        private class TaskFuture<E> implements Future<E>{

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                /* 3 options:
                    1. the task in the queue
                    2. the task in the middle of executor (with thread)
                    3. the task is done */

                /* tasks.remove(Task.this)
                    return true for option 1
                    return false for options 2+3 */

                isCanclled = removeTask(Task.this);

                /* option 2 */
                if(!isCancelled() && !isDone() && mayInterruptIfRunning){
                    currThread.interrupt();
                    isCanclled = true;
                }

                return isCanclled;
            }

            @Override
            public boolean isCancelled() {
                return isCanclled;
            }

            @Override
            public boolean isDone() {
                return isDone;
            }

            @Override
            public E get() throws InterruptedException, ExecutionException {

                try {
                    return get(Long.MAX_VALUE,TimeUnit.HOURS);
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public E get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                boolean tryAgain = false;

                try {
                    lock.lock();
                    while(!isDone() || tryAgain){
                        if(isCanclled){
                            throw new CancellationException();
                        }
                        else{
                            if(Thread.currentThread().isInterrupted()){
                                throw new InterruptedException();
                            }

                            if(executException != null){
                                throw new ExecutionException("exception from run", executException);
                            }

                            if(tryAgain){
                                break;
                            }

                            //check time out
                            if(!waitUntilDone.await(timeout,unit)){
                                throw new TimeoutException();
                            }
                            else{
                                tryAgain = true;
                            }
                        }
                    }
                } finally {
                    lock.unlock();
                }


                @SuppressWarnings("unchecked")
                E result = (E) retVal;
                return result;
            }

        }

    }

}