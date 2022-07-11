package WPQ;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class WaitablePriorityQueueCond<T> {
    private int capacity;
    private Queue<T> queue;/* needs to be PriorityQueue */
    private ReentrantLock lock;
    private Condition isFull;
    private Condition isEmpty;


    public WaitablePriorityQueueCond() {
        this(11,null);
    }

    public WaitablePriorityQueueCond(int capacity) {
        this((capacity < 0 ? 11 : capacity),null);
    }

    public WaitablePriorityQueueCond(int capacity, Comparator<? super T> comp) {
        ////check if T is comparable
        this.capacity = (capacity < 0 ? 11 : capacity);
        queue = new PriorityQueue<>(this.capacity,comp);
        lock = new ReentrantLock();
        isFull = lock.newCondition();
        isEmpty = lock.newCondition();
    }
    /* void await() throws InterruptedException
        Causes the cureent thread to wait until it is signalled or interrupted */
    public void enqueue(T data) {
        lock.lock();
        try {
            while(queue.size() == capacity){
                isFull.await();
            }
            queue.add(data);
            isEmpty.signal();//(=> the queue is not empty, so the isEmpty condition is wakes up)
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally{
            lock.unlock();
        }
    }

    public T dequeue() {
        T deData = null;

        lock.lock();
        try {
            while(isEmpty()){
                isEmpty.await();
            }
            deData = queue.poll();
            isFull.signal();//(=> the queue is not full, so the isFull condition is wakes up)
            return deData;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally{
            lock.unlock();
        }

        return deData;
    }

    public boolean remove(T data) {
        boolean isRemove = false;
        lock.lock();
        try {
            isRemove = queue.remove(data);
            isFull.signal();
            return isRemove;
        }finally{
            lock.unlock();
        }
    }

    public int size() {

        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    public boolean isEmpty() {
        lock.lock();
        try {
            return queue.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    public int getCapacity(){
        return capacity;
    }
}
