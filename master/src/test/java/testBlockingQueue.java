import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

class T{
    int i = 0;
}
class hh implements  Runnable{
    BlockingQueue<T>queue;
    int i ;
    public hh(BlockingQueue<T>queue){
        this.queue = queue;
        i = 0;
    }
    public void run(){

        try{
            queue.put(new T());
            i++;
            System.out.println("here"+"i:"+i);
        }catch (Exception e){

        }

    }
}
public class testBlockingQueue {
    BlockingQueue<T> queue = new LinkedBlockingQueue<>(10);
    public static void main(String[] args){
        testBlockingQueue testb = new testBlockingQueue();
        testb.ttt();

    }
    public void ttt(){
        ExecutorService e = Executors.newFixedThreadPool(2);

        T task = new T(); // 待添加的任务
        hh hhh = new hh(queue);
        for(int i = 0; i<10;i++){
            e.execute(hhh);
        }
        try {
            System.out.println("wait");
            e.execute(hhh);

        } catch (Exception ee) {
            // 处理中断异常

        }
    }


// 后续代码

}
