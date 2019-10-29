package JavaAPI;

import io.netty.util.concurrent.CompleteFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

public class CompleteFutureTest {

    public static void main(String[] args) throws Exception{

        //CompletableFuture<Integer> completableFuture = CompletableFuture.supplyAsync(()-> {return 1;});
        CompletableFuture<Void> completableFuture1 = CompletableFuture.runAsync(()->{System.out.println("A");});
        CompletableFuture<Void> completableFuture2 = completableFuture1.thenAccept(i->{System.out.println(i);});

       // CompletableFuture<Void> f  = completableFuture.thenAccept(System.out::println);

        //CompletableFuture<Integer> f = completableFuture.whenComplete((v,e)->{System.out.println(v);System.out.println(e);});

        //CompletableFuture<String> f = completableFuture.thenApply(i->i*10).thenApply(i->i.toString());

        //System.out.println(f.get());


        String[] strings = new String[]{"das","s","a"};
        List<String> stringList = Arrays.asList(strings);
        stringList.set(0,"q");
        System.out.println(strings[0]);
        List<String> list = stringList.subList(0,2);
        list.set(0,"w");
        System.out.println(strings[0]);

        ArrayList arrayList = new ArrayList();
        


    }

}
