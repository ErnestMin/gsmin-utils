import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class CurlUtil {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java CurlUtil <METHOD> <URL> <MESSAGE>");
            return;
        }

        String method = args[0]; // HTTP 메서드 (예: POST, GET)
        String targetUrl = args[1]; // 요청을 보낼 URL
        String message = args[2]; // 요청에 포함할 메시지 (SOAP 메시지 등)

        try {
            // URL 객체 생성
            URL url = new URL(targetUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            // HTTP 메서드 설정
            connection.setRequestMethod(method.toUpperCase());
            connection.setRequestProperty("Content-Type", "application/xml");
            connection.setRequestProperty("Accept", "application/xml");

            // 메시지 전송 설정
            if ("POST".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)) {
                connection.setDoOutput(true); // POST나 PUT일 경우에만 출력 스트림을 활성화

                try (OutputStream os = connection.getOutputStream()) {
                    byte[] input = message.getBytes();
                    os.write(input);
                }
            }

            // 응답 코드 확인
            int responseCode = connection.getResponseCode();
            String responseMessage = connection.getResponseMessage();

            System.out.println("Response Code: " + responseCode);
            System.out.println("Response Message: " + responseMessage);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
