package study;

/**
 * 定义一个MyResource类用于描述某个系统资源，实现AutoCloseable接口中的close方法，
 * 在main方法中使用try-with-resources代码对资源进行初始化并进行相关业务逻辑操作，
 * 其中try后面小括号里的一般定义资源的初始化代码，
 * 而大括号内一般写针对该资源所进行的业务逻辑操作代码，
 * 而对资源的release操作将在执行完try-with-resources后自动完成，
 * 即执行完resource.test()后自动调用close方法。这样做的好处个人认为有这么几点：
 * 1：简化代码，防止因为遗忘而导致未对资源进行释放而引起的资源浪费及其他异常，下面是我们之前常写的代码格式：
 * public void test(){
     MyResource resource = null;
     try{
     resource = new MyResource();
     resource.test();
     }finally{
     resource.close();
     }
     }

 2、防止业务代码的异常被Suppressed，如上代码，try代码块中可能会抛出异常，而finally代码块中也有可能抛出异常，
 当两个异常都同时抛出时，try代码块中所抛出的异常会被suppressed掉，所以往往捕获的异常不是我们想要的，
 而在try-with-resources代码块中的异常可以正常抛出，相反，close方法所抛出的异常会被suppressed掉。


 今天详细看了下java9的API文档，其中有几点我认为比较好的要点也一并写在这里：

 1、我们通常在写一个基类的时候尽量都可以实现AutoCloseable接口，或许并不是之后的每一个子类都会用到AutoCloseable的特性。

 2、非阻塞io（nio）没有必要用再try-with-resources代码块中。

 3、我们再重写close方法时尽量将可能抛出的异常细分（不要统一写throws Exception）。

 4、如果在close真的发生异常，根本处理好资源的状况及标识好资源的状态后再抛出异常。

 5、close方法最好不要抛出InterruptedException异常，因为InterruptedException异常会被suppressed掉，可能导致一些不希望出现的后果。

 6、不像CloseAble中的close方法，AutoCloseable中的close方法不要求是幂等的，多次调用的时候可能会出现不同的结果，但是还是建议在实现类中将close方法实现为幂等。




 */
public class AutoCloseableStudy {

    public static void main(String[] args) {
        try(MyResource resource = new MyResource()){
            resource.test();
        }
    }

}


class MyResource implements AutoCloseable{

    //在这里写关闭
    public void close() {
        System.out.println("closed");
    }

    public void test(){
        System.out.println("执行");
    }
}
