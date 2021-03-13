package spark.core.rdd;

public class Spark17_RDD_Execption_Java {
    public static void main(String[] args) {

        // 空指针异常：调用一个为空（null）对象的属性或方法，会发生空指针异常

        // 1. 属性是成员，出现空指针异常
        //    对象为null，调用成员属性，就会发生空指针异常
        // 2. 属性是静态，出现空指针异常
        //    对象为null，调用静态属性，不会发生空指针异常
        //    属性为包装类型，但是方法参数是基本类型，那么属性为null时，进行拆箱操作就会发生空指针异常.

        // sleep, wait的区别？
        // sleep静态方法
        // wait成员方法

        User user = null;
        test(user.age);
    }
    public static void test( int age ) {
        System.out.println("年龄 = " + age);
    }
}
class User {
    public static Integer age;
}
