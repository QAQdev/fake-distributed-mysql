import java.util.concurrent.*;
class Task implements Runnable {
    private int taskId;

    public Task(int taskId) {
        this.taskId = taskId;
    }

    public int getTaskId() {
        return taskId;
    }

    @Override
    public void run() {
        System.out.println("Task " + taskId + " running on thread " + Thread.currentThread().getName());
    }
}
class Result implements  Runnable{
    public  void run(){
        System.out.println("Result");
    }
}

public class TestThreadpool {
    private int nThreads;
    private ExecutorService executor;
    private BlockingQueue<Runnable> queue;

    public TestThreadpool(int nThreads) {
        this.nThreads = nThreads;
        this.executor = Executors.newFixedThreadPool(nThreads);
        this.queue = new LinkedBlockingDeque<>();
    }

    public boolean possess(Task task) {
        return queue.offer(task); // 将任务添加到阻塞队列中
    }

    public void start() {
        for (int i = 0; i < nThreads; i++) {
            executor.execute(() -> {
                try {
                    while (true) {
                        // 从阻塞队列中取出任务，并执行
                        Runnable task = queue.poll(1, TimeUnit.SECONDS);
                        if (task != null) {
                            task.run();
                        }
                    }
                } catch (InterruptedException e) {
                    System.out.println("Thread interrupted: " + Thread.currentThread().getName());
                }
            });

        }
    }
    public static void main(String[] args) {

        TestThreadpool pool = new TestThreadpool(2);
        pool.start();

        for (int i = 0; i < 10; i++) {
            Task task = new Task(i);
            boolean success = pool.possess(task);
            System.out.println("Task " + i + " submitted: " + success);
        }

        pool.shutdown();
    }
    public void shutdown() {
        executor.shutdown();
    }
}
