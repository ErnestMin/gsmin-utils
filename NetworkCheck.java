import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class NetworkCheck {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java NetworkCheck <IP> <Port>");
            return;
        }

        String ip = args[0];
        int port;

        try {
            port = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid port number: " + args[1]);
            return;
        }

        if (isPortOpen(ip, port)) {
            System.out.println("The port " + port + " on IP " + ip + " is OPEN.");
        } else {
            System.out.println("The port " + port + " on IP " + ip + " is CLOSED.");
        }
    }

    public static boolean isPortOpen(String ip, int port) {
        try (Socket socket = new Socket()) {
            // Timeout after 2 seconds
            socket.connect(new InetSocketAddress(ip, port), 2000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
