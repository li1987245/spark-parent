import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Created by jinwei on 18-1-15.
 * 退出状态码	含义
 0	命令成功退出
 > 0	在重定向或者单词展开期间(~、变量、命令、算术展开以及单词切割)失败。
 1 – 125	命令不成功退出。特定的退出值的含义，有各个命令自行定义。
 126	命令找到了，但是文件无法执行。
 127	命令没有找到
 > 128	命令因收到信号而死亡。
 */
public class ProcessTest {
    public static void main(String[] args) throws Exception {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        AtomicBoolean b = new AtomicBoolean(false);
        Process p = Runtime.getRuntime().exec("sh /home/jinwei/test.sh");
        b.set(true);
        InputStream is = p.getInputStream();
        InputStream error = p.getErrorStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(is, Charset.forName("utf-8")), 512);
        new Thread(new Runnable() {
            @Override
            public void run() {
                int count = 0;
                System.out.println("执行状态：\t" + b.get());
                while (b.get()) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    List<String> list = new ArrayList<>();
                    count += queue.drainTo(list);
                    list.forEach(System.out::println);
                }
                System.out.println("总数：\t" + count);
            }
        }).start();
        br.lines().forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
//                System.out.println("offer:" + s);
                queue.offer(s);
            }
        });
        p.waitFor();
        b.compareAndSet(true, false);
        System.out.println("退出结果:\t" + p.exitValue());
    }
}
