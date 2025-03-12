import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

public class FixedSizeChannelPool {
  private final BlockingQueue<Channel> channelPool;
  private final Connection connection;
  private final int poolSize;

  public FixedSizeChannelPool(Connection connection, int poolSize) throws IOException {
    this.connection = connection;
    this.poolSize = poolSize;
    this.channelPool = new LinkedBlockingQueue<>(poolSize);

    for (int i = 0; i < poolSize; i++) {
      channelPool.offer(createChannel());
    }
  }

  private Channel createChannel() throws IOException {
    return connection.createChannel();
  }

  public Channel borrowChannel() throws InterruptedException {
    return channelPool.take();
  }

  public void returnChannel(Channel channel) {
    if (channel != null) {
      channelPool.offer(channel);
    }
  }

  public void close() throws IOException, TimeoutException {
    for (Channel channel : channelPool) {
      channel.close();
    }
    channelPool.clear();
  }
}
