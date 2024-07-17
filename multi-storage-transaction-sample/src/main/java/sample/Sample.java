package sample;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class Sample implements AutoCloseable {

  private final DistributedTransactionManager manager;

  public Sample() throws IOException {
    // Create a transaction manager object
    TransactionFactory factory = TransactionFactory.create("database.properties");
    manager = factory.getTransactionManager();
  }

  public void loadInitialData() throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      transaction = manager.start();
      loadPlayerIfNotExists(transaction, 1, "Kageyama Tobio", 1, 1, "S",27);
      loadPlayerIfNotExists(transaction, , "", , , "",);
      loadPlayerIfNotExists(transaction, , "", , , "",);
      loadTeamIfNotExists(transaction, 1, "Karasuno");
      loadTeamIfNotExists(transaction, , "");
      loadTeamIfNotExists(transaction, , "");
      transaction.commit();
    } catch (TransactionException e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  private void loadPlayerIfNotExists(
      DistributedTransaction transaction,
      int playerID,
      String playerName,
      int teamID,
      int grade,
      String position,
      int status
      )
      throws TransactionException {
    Optional<Result> player =
        transaction.get(
            Get.newBuilder()
                .namespace("team")
                .table("players")
                .partitionKey(Key.ofInt("player_id", playerID))
                .build());
    if (!player.isPresent()) {
      transaction.put(
          Put.newBuilder()
              .namespace("team")
              .table("players")
              .partitionKey(Key.ofInt("player_id", playerID))
              .textValue("player_name", playerName)
              .intValue("team_id", teamID)
              .intValue("grade", grade)
              .textValue("position", position)
              .intValue("status", status)
              .build());
    }
  }

  private void loadTeamIfNotExists(
      DistributedTransaction transaction, 
      int teamID,
      String teamName
      )
      throws TransactionException {
    Optional<Result> team =
        transaction.get(
            Get.newBuilder()
                .namespace("team")
                .table("teams")
                .partitionKey(Key.ofInt("team_id", teamID))
                .build());
    if (!team.isPresent()) {
      transaction.put(
          Put.newBuilder()
              .namespace("team")
              .table("teams")
              .partitionKey(Key.ofInt("team_id", teamID))
              .textValue("team_name", teamName)
              .build());
    }
  }

  public String getPlayerInfo(int playerID) throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      // Start a transaction
      transaction = manager.start();

      // Retrieve the player info for the specified player ID from the players table
      Optional<Result> player =
          transaction.get(
              Get.newBuilder()
                  .namespace("team")
                  .table("players")
                  .partitionKey(Key.ofInt("player_id", playerID))
                  .build());

      if (!player.isPresent()) {
        // If the player info the specified player ID doesn't exist, throw an exception
        throw new RuntimeException("Player not found");
      }

      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.commit();

      // Return the player info as a JSON format
      return String.format(
          "{\"id\": %d, \"name\": \"%s\", \"credit_limit\": %d, \"credit_total\": %d}",
          playerID,
          player.get().getText("name"),
          player.get().getInt("credit_limit"),
          player.get().getInt("credit_total"));
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  // get and display MATCH information from TEAM NAME
  public String getMatchInfo(int teamName) throws TransactionException {

  }

  public String getMatchInfo(int team, String match_nickname) throws TransactionException {

  }

  public String placeOrder(int customerId, int[] itemIds, int[] itemCounts)
      throws TransactionException {
    assert itemIds.length == itemCounts.length;

    DistributedTransaction transaction = null;
    try {
      String orderId = UUID.randomUUID().toString();

      // Start a transaction
      transaction = manager.start();

      // Put the order info into the orders table
      transaction.put(
          Put.newBuilder()
              .namespace("order")
              .table("orders")
              .partitionKey(Key.ofInt("customer_id", customerId))
              .clusteringKey(Key.ofBigInt("timestamp", System.currentTimeMillis()))
              .textValue("order_id", orderId)
              .build());

      int amount = 0;
      for (int i = 0; i < itemIds.length; i++) {
        int itemId = itemIds[i];
        int count = itemCounts[i];

        // Put the order statement into the statements table
        transaction.put(
            Put.newBuilder()
                .namespace("order")
                .table("statements")
                .partitionKey(Key.ofText("order_id", orderId))
                .clusteringKey(Key.ofInt("item_id", itemId))
                .intValue("count", count)
                .build());

        // Retrieve the item info from the items table
        Optional<Result> item =
            transaction.get(
                Get.newBuilder()
                    .namespace("order")
                    .table("items")
                    .partitionKey(Key.ofInt("item_id", itemId))
                    .build());

        if (!item.isPresent()) {
          throw new RuntimeException("Item not found");
        }

        // Calculate the total amount
        amount += item.get().getInt("price") * count;
      }

      // Check if the credit total exceeds the credit limit after payment
      Optional<Result> customer =
          transaction.get(
              Get.newBuilder()
                  .namespace("customer")
                  .table("customers")
                  .partitionKey(Key.ofInt("customer_id", customerId))
                  .build());
      if (!customer.isPresent()) {
        throw new RuntimeException("Customer not found");
      }
      int creditLimit = customer.get().getInt("credit_limit");
      int creditTotal = customer.get().getInt("credit_total");
      if (creditTotal + amount > creditLimit) {
        throw new RuntimeException("Credit limit exceeded");
      }

      // Update credit_total for the customer
      transaction.put(
          Put.newBuilder()
              .namespace("customer")
              .table("customers")
              .partitionKey(Key.ofInt("customer_id", customerId))
              .intValue("credit_total", creditTotal + amount)
              .build());

      // Commit the transaction
      transaction.commit();

      // Return the order id
      return String.format("{\"order_id\": \"%s\"}", orderId);
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  private String getOrderJson(DistributedTransaction transaction, String orderId)
      throws TransactionException {
    // Retrieve the order info for the order ID from the orders table
    Optional<Result> order =
        transaction.get(
            Get.newBuilder()
                .namespace("order")
                .table("orders")
                .indexKey(Key.ofText("order_id", orderId))
                .build());

    if (!order.isPresent()) {
      throw new RuntimeException("Order not found");
    }

    int customerId = order.get().getInt("customer_id");

    // Retrieve the customer info for the specified customer ID from the customers table
    Optional<Result> customer =
        transaction.get(
            Get.newBuilder()
                .namespace("customer")
                .table("customers")
                .partitionKey(Key.ofInt("customer_id", customerId))
                .build());
    assert customer.isPresent();

    // Retrieve the order statements for the order ID from the statements table
    List<Result> statements =
        transaction.scan(
            Scan.newBuilder()
                .namespace("order")
                .table("statements")
                .partitionKey(Key.ofText("order_id", orderId))
                .build());

    // Make the statements JSONs
    List<String> statementJsons = new ArrayList<>();
    int total = 0;
    for (Result statement : statements) {
      int itemId = statement.getInt("item_id");

      // Retrieve the item data from the items table
      Optional<Result> item =
          transaction.get(
              Get.newBuilder()
                  .namespace("order")
                  .table("items")
                  .partitionKey(Key.ofInt("item_id", itemId))
                  .build());

      if (!item.isPresent()) {
        throw new RuntimeException("Item not found");
      }

      int price = item.get().getInt("price");
      int count = statement.getInt("count");

      statementJsons.add(
          String.format(
              "{\"item_id\": %d,\"item_name\": \"%s\",\"price\": %d,\"count\": %d,\"total\": %d}",
              itemId, item.get().getText("name"), price, count, price * count));

      total += price * count;
    }

    // Return the order info as a JSON format
    return String.format(
        "{\"order_id\": \"%s\",\"timestamp\": %d,\"customer_id\": %d,\"customer_name\": \"%s\",\"statement\": [%s],\"total\": %d}",
        orderId,
        order.get().getBigInt("timestamp"),
        customerId,
        customer.get().getText("name"),
        String.join(",", statementJsons),
        total);
  }

  public String getOrderByOrderId(String orderId) throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      // Start a transaction
      transaction = manager.start();

      // Get an order JSON for the specified order ID
      String orderJson = getOrderJson(transaction, orderId);

      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.commit();

      // Return the order info as a JSON format
      return String.format("{\"order\": %s}", orderJson);
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public String getOrdersByCustomerId(int customerId) throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      // Start a transaction
      transaction = manager.start();

      // Retrieve the order info for the customer ID from the orders table
      List<Result> orders =
          transaction.scan(
              Scan.newBuilder()
                  .namespace("order")
                  .table("orders")
                  .partitionKey(Key.ofInt("customer_id", customerId))
                  .build());

      // Make order JSONs for the orders of the customer
      List<String> orderJsons = new ArrayList<>();
      for (Result order : orders) {
        orderJsons.add(getOrderJson(transaction, order.getText("order_id")));
      }

      // Commit the transaction (even when the transaction is read-only, we need to commit)
      transaction.commit();

      // Return the order info as a JSON format
      return String.format("{\"order\": [%s]}", String.join(",", orderJsons));
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  public void repayment(int customerId, int amount) throws TransactionException {
    DistributedTransaction transaction = null;
    try {
      // Start a transaction
      transaction = manager.start();

      // Retrieve the customer info for the specified customer ID from the customers table
      Optional<Result> customer =
          transaction.get(
              Get.newBuilder()
                  .namespace("customer")
                  .table("customers")
                  .partitionKey(Key.ofInt("customer_id", customerId))
                  .build());
      if (!customer.isPresent()) {
        throw new RuntimeException("Customer not found");
      }

      int updatedCreditTotal = customer.get().getInt("credit_total") - amount;

      // Check if over repayment or not
      if (updatedCreditTotal < 0) {
        throw new RuntimeException("Over repayment");
      }

      // Reduce credit_total for the customer
      transaction.put(
          Put.newBuilder()
              .namespace("customer")
              .table("customers")
              .partitionKey(Key.ofInt("customer_id", customerId))
              .intValue("credit_total", updatedCreditTotal)
              .build());

      // Commit the transaction
      transaction.commit();
    } catch (Exception e) {
      if (transaction != null) {
        // If an error occurs, abort the transaction
        transaction.abort();
      }
      throw e;
    }
  }

  @Override
  public void close() {
    manager.close();
  }
}
