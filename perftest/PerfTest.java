import java.lang.management.*;
import java.math.BigInteger;

public class PerfTest {

   public static ThreadMXBean mxb;
  
   public static void main(String[] args){
     mxb = ManagementFactory.getThreadMXBean();

     System.out.println(("Current thread count:"+ mxb.getThreadCount()));

     factInt(12);
     factLong(12);
     factBigInt("12");

   }

   public static void factBigInt(String f) {

     long cTime = mxb.getCurrentThreadCpuTime();

      // factorial
     BigInteger factor = new BigInteger(f);
     BigInteger i = factor;
     BigInteger result = BigInteger.ONE;
     
     while(i.compareTo(BigInteger.ZERO) != 0){
        if( i.compareTo(BigInteger.ONE) == 0)
          System.out.println("FAC " + i + " " + result);
        result = result.multiply(i);
        i = i.subtract(BigInteger.ONE);
     }
     long nTime = mxb.getCurrentThreadCpuTime();
     System.out.println(("Total CPU time (ns):" + (nTime - cTime)));
     System.out.println(("Total CPU time (ms):" + (nTime - cTime) / 1000000));
     System.out.println(("Total CPU time (s):" + (nTime - cTime) / 1000000000L));

   }

   public static void factLong(long f) {

     long cTime = mxb.getCurrentThreadCpuTime();

      // factorial
     long factor = f;
     long i = factor;
     long result = 1;
     
     while(i != 0){
        if( i == 1)
          System.out.println("FAC " + i + " " + result);
        result *= i;
        i-=1;
     }
     long nTime = mxb.getCurrentThreadCpuTime();
     System.out.println(("Total CPU time (ns):" + (nTime - cTime)));
     System.out.println(("Total CPU time (ms):" + (nTime - cTime) / 1000000));
     System.out.println(("Total CPU time (s):" + (nTime - cTime) / 1000000000L));

   }

   public static void factInt(int f) {

     long cTime = mxb.getCurrentThreadCpuTime();

      // factorial
     int factor = f;
     int i = factor;
     int result = 1;
     
     while(i != 0){
        if( i == 1)
          System.out.println("FAC " + i + " " + result);
        result *= i;
        i-=1;
     }
     long nTime = mxb.getCurrentThreadCpuTime();
     System.out.println(("Total CPU time (ns):" + (nTime - cTime)));
     System.out.println(("Total CPU time (ms):" + (nTime - cTime) / 1000000));
     System.out.println(("Total CPU time (s):" + (nTime - cTime) / 1000000000L));

   }
}
