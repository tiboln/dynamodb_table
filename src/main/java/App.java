import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.util.concurrent.RateLimiter;

public class App {

    public static void main(String[] args) {
        final AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder
                .standard()
                .withRegion("eu-west-3")
                .build();

        final String tableName = "chat-table";
        final RateLimiter rateLimiter = RateLimiter.create(1);
        // Busy loop the application
        while(true) {
            // Create table
            CreateTableRequest ctr = new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(new KeySchemaElement("chat-id", KeyType.HASH))
                    .withAttributeDefinitions(new AttributeDefinition("chat-id", ScalarAttributeType.S))
                    .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L));

            try {
                dynamoDB.createTable(ctr);
            } catch (ResourceInUseException riue) {
                System.err.println("Table already in use: " + riue.getMessage());
            }

            // Describe table
            DescribeTableResult dtr = null;
            while (dtr == null || !dtr.getTable().getTableStatus().equals("ACTIVE")) {
                try {
                    dtr = dynamoDB.describeTable(tableName);
                } catch (Exception e) {
                    if (rateLimiter.tryAcquire()) {
                        System.err.println("Failed to describe table: " + e.getMessage());
                    }
                }
            }

            // Delete table
            try {
                dynamoDB.deleteTable(tableName);
            } catch (Exception e) {
                System.err.println("Failed to delete table: " + e.getMessage());
            }

            // Describe table
            while (true) {
                try {
                    dynamoDB.describeTable(tableName);
                } catch (ResourceNotFoundException rnfe) {
                    break;
                } catch (Exception e) {
                    if (rateLimiter.tryAcquire()) {
                        System.err.println("Failed to describe table: " + e.getMessage());
                    }
                }
            }
        }
    }
}