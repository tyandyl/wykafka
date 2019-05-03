package study;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tianye13 on 2019/4/10.
 */
public class Unmodifiable {

    public static void main(String[] agrs){
               Map map = new HashMap();
                map.put("name", "zhangchengzi");
                map.put("age", 20);

                 System.out.println("map before:"+map);//打印结果：map before:{name=zhangchengzi, age=20}

                 Map unmodifiableMap = Collections.unmodifiableMap(map);
                 System.out.println("unmodifiableMap before:"+unmodifiableMap);//打印结果：unmodifiableMap before:{name=zhangchengzi, age=20}。

                System.out.println("年龄："+unmodifiableMap.get("age"));//打印结果：年龄：20
                //修改年龄
                 unmodifiableMap.put("age", 28);

                 System.out.println("map after:"+map);
                System.out.println("unmodifiableMap after:"+unmodifiableMap);
    }
}
