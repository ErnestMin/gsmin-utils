import java.io.IOException;
import java.net.InetAddress;

public class Ping {
    public static void main(String[] args) {
        String host = "10.3.88.19";  // 핑을 보낼 대상
        int timeout = 1000;  // 타임아웃 시간 (밀리초 단위)
        int count = 10;  // 핑을 보낼 횟수

        try {
            InetAddress address = InetAddress.getByName(host);
            System.out.println("Pinging " + host + " with " + count + " requests...");

            // 반복적으로 ping을 보내고 응답 시간을 출력
            for (int i = 1; i <= count; i++) {
                long startTime = System.currentTimeMillis();  // 시작 시간
                boolean reachable = address.isReachable(timeout);
                long endTime = System.currentTimeMillis();  // 끝 시간

                if (reachable) {
                    long roundTripTime = endTime - startTime;  // 왕복 시간 계산
                    System.out.println("Reply from " + host + ": time=" + roundTripTime + "ms");
                } else {
                    System.out.println("Request timed out.");
                }

                // 1초 간격으로 반복 (ping 명령어와 유사)
                Thread.sleep(1000);
            }
        } catch (IOException e) {
            System.out.println("Error occurred while pinging " + host);
            e.printStackTrace();
        } catch (InterruptedException e) {
            System.out.println("Ping interrupted.");
            e.printStackTrace();
        }
    }
}
