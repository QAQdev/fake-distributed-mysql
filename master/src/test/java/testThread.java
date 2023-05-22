import static java.lang.Thread.sleep;

class tt implements  Runnable{
    int a = 1;
    public tt(int aa){
        this.a = aa;
    }
    public void run(){
        while (true){
            this.a ++;
            try {
                sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println(this.a);
        }
    }

}
public class testThread {
    public static void main(String[] args){
        tt ttt = new tt(11);
        tt tt1 = new tt(22);
        Thread t = new Thread(tt1);
        t.start();
        Thread t1 = new Thread(tt1);
        t1.start();
    }



}
