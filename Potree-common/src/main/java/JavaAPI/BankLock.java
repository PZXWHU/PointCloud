package JavaAPI;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BankLock {

    private final double[] accounts;

    private Lock bankLock = new ReentrantLock();
    private Condition sufficientFunds;//条件变量

    public BankLock(int n, double initialBalance){
        accounts = new double[n];
        Arrays.fill(accounts,initialBalance);
        sufficientFunds = bankLock.newCondition();//一个锁可以有很多条件变量
    }



    public  void transfer(int from,int to,double amount)throws Exception{
        bankLock.lock();
        try {
            while (accounts[from]<amount)
                sufficientFunds.await();//线程堵塞，并放弃了锁。直到另一线程上同一条件对象调用singalALL（），此线程重新加入锁池



            System.out.println(Thread.currentThread());
            accounts[from] -= amount;
            accounts[to] += amount;
            System.out.println("Total Balance:"+getTotalBalance());
        }finally {
            bankLock.unlock();
        }

    }


    public synchronized void transfer(){
        //每一个对象都有一个内部锁
        //如果方法用synchronized关键字声明，那么对象的锁将保护整个方法
        //也就是调用此方法必须要获取内部的对象锁


        //此方法等价于
        //this.intrinsicLock.lock();
        try{
            //method body

            //对象锁只有一个相关条件
            //调用wait方法，将线程加入到等待集中
            //调用notify//notifyall接触线程的等待状态

        }finally {
            //this.intrinsicLock.unlock();
        }

    }





    public double getTotalBalance(){
        double sum = 0;
        for(double a:accounts)
            sum+=a;
        return sum;
    }

    public int add(int a,int b){
        return a+b;
    }

}
