package JavaAPI;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public class ThreadAPI {

    public static void main(String[] args) throws Exception{

        //启动线程
        //线程运行的方式：调用Thread.start()函数开启线程
        //start函数调用run函数，执行线程任务
        //执行线程任务有两种方式：1.继承Thread类，改写run函数，直接执行run内容
        //2.run函数原本内容是执行target.run（），所以在创建Thread的时候需要传入一个Runnable的target。所以这个target可以是Runnable匿名类，Runnable接口实现类，甚至Thread对象，因为Thread类实现了Runnable接口


        //1.匿名类方式

        //匿名runnable
        Thread t1 = new Thread(()->System.out.println(Thread.currentThread().getName()),"t1") ;
        t1.start();

        Thread t2 = new Thread("t2"){
            @Override
            public void run() {
                System.out.println(Thread.currentThread().getName());
            }
        };
        t2.start();

        Thread t3 = new Thread(new Thread(){
            @Override
            public void run() {
                System.out.println(Thread.currentThread().getName());
            }
        },"t3");
        t3.start();



        //2.继承以及实现方式

        Thread2 t4 = new Thread2("t4");
        t4.start();

        Thread t5 = new Thread(new RunImp(),"t5");
        t5.start();



        //通过 Callable 和 Future 创建线程
        FutureTask<Integer> futureTask = new FutureTask<>(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                System.out.println(Thread.currentThread().getName());
                return 7;
            }
        });
        Thread t6 = new Thread(futureTask,"t6");
        t6.start();
        System.out.println(futureTask.get());


    }


    public static class Thread2 extends Thread{
        @Override
        public void run() {
            System.out.println(Thread.currentThread().getName());
        }

        public Thread2(String name) {
            super(name);
        }
    }

    public static class RunImp implements Runnable{
        @Override
        public void run() {
            System.out.println(Thread.currentThread().getName());
        }
    }


}
