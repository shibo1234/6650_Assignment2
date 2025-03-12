//import com.google.gson.Gson;
//import com.google.gson.JsonObject;
//import com.rabbitmq.client.Channel;
//import com.rabbitmq.client.Connection;
//import com.rabbitmq.client.ConnectionFactory;
//import java.io.BufferedReader;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ThreadLocalRandom;
//import java.util.concurrent.TimeUnit;
//import javax.servlet.*;
//import javax.servlet.http.*;
//import java.io.IOException;
//
//public class SkierServlet extends HttpServlet {
//
//  private final Gson gson = new Gson();
//
////  private static final String QUEUE_NAME = "LiftRideQueue";
//
//  private static final int CHANNEL_COUNT = 255;
//
//  private static final String RABBITMQ_HOST = "50.16.76.56";
//  private static final String USERNAME = "admin";
//  private static final String PASSWORD = "admin";
//
//  private ExecutorService executorService;
//  private Connection connection;
//  private ThreadLocal<Channel> threadLocalChannel;
//
//  private static final int QUEUE_COUNT = 20;
//  private static final String EXCHANGE_NAME = "A2_directExchange";
//
//  @Override
//  public void init() throws ServletException {
//    try {
//      executorService = Executors.newFixedThreadPool(200);
//      ConnectionFactory factory = new ConnectionFactory();
//      factory.setHost(RABBITMQ_HOST);
//      factory.setUsername(USERNAME);
//      factory.setPassword(PASSWORD);
//
//      connection = factory.newConnection();
//      threadLocalChannel = ThreadLocal.withInitial(() -> {
//        try {
//          Channel channel = connection.createChannel();
////          channel.queueDeclare(QUEUE_NAME, false, false, false, null);
//          channel.exchangeDeclare(EXCHANGE_NAME, "direct", false);
//
//          for (int i = 0; i < QUEUE_COUNT; i++) {
//            String queueName = "queue_" + i;
//            channel.queueDeclare(queueName, false, false, false, null);
//            channel.queueBind(queueName, EXCHANGE_NAME, queueName);
//          }
//
//          return channel;
//        } catch (Exception e) {
//          throw new RuntimeException("Failed to create channel", e);
//        }
//      });
//      System.out.println("RabbitMQ connection established and multiple channels initialized.");
//    } catch (Exception e) {
//      throw new ServletException("Failed to initialize RabbitMQ", e);
//    }
//  }
//
//
//  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
//    res.setContentType("text/plain");
//    String urlPath = req.getPathInfo();
//
//    // check we have a URL!
//    if (urlPath == null || urlPath.isEmpty()) {
//      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
//      res.getWriter().write("missing paramterers");
//      return;
//    }
//
//    String[] urlParts = urlPath.split("/");
//    // and now validate url path and return the response status code
//    // (and maybe also some value if input is valid)
//
//    if (!isUrlValid(urlParts)) {
//      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
//    } else {
//      res.setStatus(HttpServletResponse.SC_OK);
//      // do any sophisticated processing with urlParts which contains all the url params
//      // TODO: process url params in `urlParts`
//      res.getWriter().write("It works!");
//    }
//  }
//
//  @Override
//  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
//    resp.setContentType("application/json");
//    String urlPath = req.getPathInfo();
//
//    if (urlPath == null || urlPath.isEmpty()) {
//      resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
//      resp.getWriter().write("{\"message\": \"missing parameters\"}");
//      return;
//    }
//
//    String[] urlParts = urlPath.split("/");
//
//    if (!isUrlValid(urlParts)) {
//      System.out.println("Invalid URL");
//      resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
//      resp.getWriter().write("{\"message\": \"Invalid URL\"}");
//      return;
//    }
//
//    StringBuilder jsonPayLoad = new StringBuilder();
//    String line;
//
//    try (BufferedReader reader = req.getReader()) {
//      while ((line = reader.readLine()) != null) {
//        jsonPayLoad.append(line);
//      }
//    }
//
////    System.out.println("Parsed JSON Payload: " + jsonPayLoad);
//
//    JsonObject jsonObject;
//    try {
//      jsonObject = gson.fromJson(jsonPayLoad.toString(), JsonObject.class);
//    } catch (Exception e) {
//      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
//      resp.getWriter().write("{\"message\": \"Invalid JSON\"}");
//      return;
//    }
//
//    if (!isJsonValid(jsonObject)) {
//      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
//      resp.getWriter().write("{\"message\": \"Invalid JSON\"}");
//      return;
//    }
//
//    int resortID = Integer.parseInt(urlParts[1]);
////    String seasonKeyWord = urlParts[2];
//    int seasonID = Integer.parseInt(urlParts[3]);
////    String dayKeyWord = urlParts[4];
//    int dayID = Integer.parseInt(urlParts[5]);
////    String skierKeyWord = urlParts[6];
//    int skierID = Integer.parseInt(urlParts[7]);
//
//    JsonObject completeJson = new JsonObject();
//    completeJson.addProperty("resortID", resortID);
//    completeJson.addProperty("seasonID", seasonID);
//    completeJson.addProperty("dayID", dayID);
//    completeJson.addProperty("skierID", skierID);
//    completeJson.add("liftRide", jsonObject);
//
////    sendToQueue(completeJson.toString());
//    executorService.submit(() -> sendToQueue(completeJson.toString()));
//
//    resp.setStatus(HttpServletResponse.SC_CREATED);
//    resp.getWriter().write("{\"message\": \"Lift ride created successfully\"}");
//  }
//
//  private boolean isUrlValid(String[] urlPath) {
//    try {
//      int resortID = Integer.parseInt(urlPath[1]);
//      String seasonKeyWord = urlPath[2];
//      int seasonID = Integer.parseInt(urlPath[3]);
//      String dayKeyWord = urlPath[4];
//      int dayID = Integer.parseInt(urlPath[5]);
//      String skierKeyWord = urlPath[6];
//      int skierID = Integer.parseInt(urlPath[7]);
//
//      return resortID >= 0 && resortID <= 10 &&
//          seasonKeyWord.equals("seasons") &&
//          seasonID == 2025 &&
//          dayKeyWord.equals("days") &&
//          dayID == 1 &&
//          skierKeyWord.equals("skiers") &&
//          skierID >= 0 && skierID <= 10_0000;
//    } catch (Exception e) {
//      return false;
//    }
//  }
//
//  private boolean isJsonValid(JsonObject json) {
//    try {
//      int liftID = json.get("liftID").getAsInt();
//      int time = json.get("time").getAsInt();
//
//      return liftID >= 1 && liftID <= 40 &&
//          time >= 1 && time <= 360;
//    } catch (Exception e) {
//      return false;
//    }
//  }
//
//  private void sendToQueue(String message) {
//    try {
//      Channel channel = threadLocalChannel.get();
////      channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
//
//      int queueIndex = ThreadLocalRandom.current().nextInt(QUEUE_COUNT);
//      String routingKey = "queue_" + queueIndex;
//
//      channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
////      System.out.println(" [x] Sent to " + routingKey + ": " + message);
//
//    } catch (Exception e) {
//      System.err.println(" [!] Failed to send message: " + e.getMessage());
//    }
//  }
//
//  @Override
//  public void destroy() {
//    try {
//      System.out.println("Shutting down ExecutorService...");
//      executorService.shutdown();
//      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
//        executorService.shutdownNow();
//      }
//
//      System.out.println("Closing RabbitMQ connection...");
//      if (connection != null && connection.isOpen()) {
//        connection.close();
//      }
//    } catch (IOException | InterruptedException e) {
//      System.out.println("Failed to close resources: " + e.getMessage());
//      Thread.currentThread().interrupt();
//    }
//  }
//}

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.BufferedReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.IOException;

public class SkierServlet extends HttpServlet {

  private final Gson gson = new Gson();
  private static final int CHANNEL_COUNT = 255;
  private static final int QUEUE_COUNT = 100;
  private static final String EXCHANGE_NAME = "A2_directExchange";
  private static final String RABBITMQ_HOST = "172.31.22.60";
  private static final String USERNAME = "admin";
  private static final String PASSWORD = "admin";

  private ExecutorService executorService;
  private Connection connection;
  private FixedSizeChannelPool channelPool;

  @Override
  public void init() throws ServletException {
    try {
      executorService = Executors.newFixedThreadPool(200);
      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost(RABBITMQ_HOST);
      factory.setUsername(USERNAME);
      factory.setPassword(PASSWORD);

      connection = factory.newConnection();
      channelPool = new FixedSizeChannelPool(connection, CHANNEL_COUNT);
      initializeExchangeAndQueues();

      System.out.println("RabbitMQ connection established and channel pool initialized.");
    } catch (Exception e) {
      throw new ServletException("Failed to initialize RabbitMQ", e);
    }
  }

  private void initializeExchangeAndQueues() throws Exception {
    try (Channel channel = connection.createChannel()) {
      channel.exchangeDeclare(EXCHANGE_NAME, "direct", false);
      for (int i = 0; i < QUEUE_COUNT; i++) {
        String queueName = "queue_" + i;
        channel.queueDeclare(queueName, false, false, false, null);
        channel.queueBind(queueName, EXCHANGE_NAME, queueName);
      }
    }
  }

  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    res.setContentType("text/plain");
    String urlPath = req.getPathInfo();

    // check we have a URL!
    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("missing paramterers");
      return;
    }

    String[] urlParts = urlPath.split("/");
    // and now validate url path and return the response status code
    // (and maybe also some value if input is valid)

    if (!isUrlValid(urlParts)) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
    } else {
      res.setStatus(HttpServletResponse.SC_OK);
      // do any sophisticated processing with urlParts which contains all the url params
      // TODO: process url params in `urlParts`
      res.getWriter().write("It works!");
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    resp.setContentType("application/json");
    String urlPath = req.getPathInfo();

    if (urlPath == null || urlPath.isEmpty()) {
      resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
      resp.getWriter().write("{\"message\": \"missing parameters\"}");
      return;
    }

    String[] urlParts = urlPath.split("/");
    if (!isUrlValid(urlParts)) {
      resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
      resp.getWriter().write("{\"message\": \"Invalid URL\"}");
      return;
    }

    StringBuilder jsonPayload = new StringBuilder();
    String line;
    try (BufferedReader reader = req.getReader()) {
      while ((line = reader.readLine()) != null) {
        jsonPayload.append(line);
      }
    }

    JsonObject jsonObject;
    try {
      jsonObject = gson.fromJson(jsonPayload.toString(), JsonObject.class);
    } catch (Exception e) {
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      resp.getWriter().write("{\"message\": \"Invalid JSON\"}");
      return;
    }

    if (!isJsonValid(jsonObject)) {
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      resp.getWriter().write("{\"message\": \"Invalid JSON\"}");
      return;
    }

    int resortID = Integer.parseInt(urlParts[1]);
    int seasonID = Integer.parseInt(urlParts[3]);
    int dayID = Integer.parseInt(urlParts[5]);
    int skierID = Integer.parseInt(urlParts[7]);

    JsonObject completeJson = new JsonObject();
    completeJson.addProperty("resortID", resortID);
    completeJson.addProperty("seasonID", seasonID);
    completeJson.addProperty("dayID", dayID);
    completeJson.addProperty("skierID", skierID);
    completeJson.add("liftRide", jsonObject);

    executorService.submit(() -> sendToQueue(completeJson.toString()));

    resp.setStatus(HttpServletResponse.SC_CREATED);
    resp.getWriter().write("{\"message\": \"Lift ride created successfully\"}");
  }

  private boolean isJsonValid(JsonObject json) {
    try {
      int liftID = json.get("liftID").getAsInt();
      int time = json.get("time").getAsInt();

      return liftID >= 1 && liftID <= 40 &&
          time >= 1 && time <= 360;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean isUrlValid(String[] urlPath) {
    try {
      int resortID = Integer.parseInt(urlPath[1]);
      String seasonKeyWord = urlPath[2];
      int seasonID = Integer.parseInt(urlPath[3]);
      String dayKeyWord = urlPath[4];
      int dayID = Integer.parseInt(urlPath[5]);
      String skierKeyWord = urlPath[6];
      int skierID = Integer.parseInt(urlPath[7]);

      return resortID >= 0 && resortID <= 10 &&
          seasonKeyWord.equals("seasons") &&
          seasonID == 2025 &&
          dayKeyWord.equals("days") &&
          dayID == 1 &&
          skierKeyWord.equals("skiers") &&
          skierID >= 0 && skierID <= 10_0000;
    } catch (Exception e) {
      return false;
    }
  }

  private void sendToQueue(String message) {
    Channel channel = null;
    try {
      channel = channelPool.borrowChannel();
      int queueIndex = ThreadLocalRandom.current().nextInt(QUEUE_COUNT);
      String routingKey = "queue_" + queueIndex;
      channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
    } catch (Exception e) {
      System.err.println(" [!] Failed to send message: " + e.getMessage());
    } finally {
      if (channel != null) {
        channelPool.returnChannel(channel);
      }
    }
  }

  @Override
  public void destroy() {
    try {
      System.out.println("Shutting down ExecutorService...");
      executorService.shutdown();
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }

      System.out.println("Closing RabbitMQ connection...");
      channelPool.close();
      if (connection != null && connection.isOpen()) {
        connection.close();
      }
    } catch (IOException | InterruptedException e) {
      System.out.println("Failed to close resources: " + e.getMessage());
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
  }
}
