package spark.core.rdd;

import java.util.Random;

public class Spark26_RDD_Random_Java {
    public static void main(String[] args) {

        Random r1 = new Random(10);
        for ( int i = 0; i < 5; i++ ) {
            System.out.println(r1.nextInt(10));
        }
        System.out.println("**********************");
        Random r2 = new Random(10);
        for ( int i = 0; i < 5; i++ ) {
            System.out.println(r2.nextInt(10));
        }
    }

}
