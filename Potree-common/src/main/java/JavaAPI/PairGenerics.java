package JavaAPI;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import javax.xml.validation.Schema;
import java.util.ArrayList;

public class PairGenerics<T>   {

    private T first;
    private T second;

    public PairGenerics(){first = null;second = null;}
    public PairGenerics(T first, T second){
        this.first = first;
        this.second = second;
    }

    public T getFirst(){return first;}
    public T getSecond(){return second;}

    public void setFirst(T first){this.first = first;}
    public void setSecond(T second){this.second = second;}

    public static <T extends Comparable<? super T>> T min(T[] a){
        if(a==null||a.length==0) return null;
        T smallest = a[0];
        for(int i=0;i<a.length;i++){
            if(smallest.compareTo(a[i])>0) smallest = a[i];
        }
        return  smallest;
    }

    public T f(T a){
        T[] array = (T[]) new Object[2];
        array[0] = a;
        array[1] = (T)"das";
        String b = (String)a;

        System.out.println(array[1]);
        return array[1];
    }

    public T g(){
        return (T)new Object();
    }


    public static void main(String[] args){
        java.lang.Object[] objects = null;



        objects = new String[2];
        objects[0] = 1;


    }

}
