package study;

import java.util.*;

/**
 * Created by tianye13 on 2019/4/10.
 */
public class Singleton {
    public static void main(String[] agrs){
        Set set1 =new HashSet();
        set1.add(111);
        set1.add(11.11);
        set1.add("sss");
        for(Iterator it = set1.iterator(); it.hasNext(); )
        {
            System.out.println("value="+it.next().toString());
        }
        //set可以存放各种元素，然后使用迭代器进行遍历，输出

        String init[] = { "One", "Two", "Three", "One", "Two", "Three" };
        List list1 = new ArrayList(Arrays.asList(init));
        List list2 = new ArrayList(Arrays.asList(init));
        list1.remove("One");  //只会移除第一个元素
        list1.add("One");
        System.out.println("List1 value: "+list1);
        //List1 value: [Two, Three, One, Two, Three, One]

        list2.removeAll(Collections.singleton("One"));
        System.out.println(Collections.singleton("One"));
        //[One]
        System.out.println("The SingletonList is :"+list2);
        //The SingletonList is :[Two, Three, Two, Three]
    }
}
