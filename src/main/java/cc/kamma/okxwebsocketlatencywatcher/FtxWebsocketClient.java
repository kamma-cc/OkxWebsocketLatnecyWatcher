package cc.kamma.okxwebsocketlatencywatcher;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

@Component
public class FtxWebsocketClient implements ApplicationRunner, Runnable, WebSocket.Listener {


    private final MeterRegistry meterRegistry;
    private final Timer timer;

    public FtxWebsocketClient(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.timer = Timer.builder("latency")
                .tag("site", "FTX")
                .publishPercentiles(0.99, 0.999, 0.9999)
                .maximumExpectedValue(Duration.ofMillis(80))
                .minimumExpectedValue(Duration.ofMillis(1))
                .sla(Duration.ofMillis(10))
                .register(meterRegistry);

    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        new Thread(this).start();
    }

    @Override
    public void run() {


        HttpClient httpClient = HttpClient.newHttpClient();
        httpClient.newWebSocketBuilder().buildAsync(
                URI.create("wss://ftx.com/ws/"), this
        );
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        System.out.println("opened");
        webSocket.sendText(
                """
                        {"op": "subscribe", "channel": "ticker", "market": "BTC-PERP"}"""
                , true);
        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        if (last) {
            String str = data.toString();
            int length = data.length();
            int index = str.indexOf("time");
            String tsStr = str.substring(index + 6, index+20);
            try {
                double ts = Double.parseDouble(tsStr);
                long latencyMs = System.currentTimeMillis() - (long) (ts * 1000);
                System.out.println("ftx:" + latencyMs);
                timer.record(latencyMs, TimeUnit.MILLISECONDS);
            } catch (NumberFormatException e) {
                // ignore
            }

        }
        webSocket.request(1);
        return null;
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        System.out.println("closed");
        return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        System.out.println("error: " + error);
    }

}
