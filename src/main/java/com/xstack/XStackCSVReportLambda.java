package com.xstack;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;

public class XStackCSVReportLambda implements RequestHandler<Object, String> {
    private static final String BUCKET_NAME = System.getenv("BUCKET_NAME");
    private static final String DYNAMO_TABLE_NAME = System.getenv("DYNAMO_TABLE_NAME");

    private final AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().build();
    private final DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
    private final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

    @Override
    public String handleRequest(Object input, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Function execution started on: " + java.time.LocalDateTime.now());

        try {
            ByteArrayOutputStream reportStream = generateCSVReport(logger);
            uploadReportToS3(reportStream, logger);
            logger.log("Report Generated and Uploaded Successfully");
            return "Report Generated and Uploaded Successfully";
        } catch (Exception e) {
            logger.log("Error in generating and uploading report: " + e.getMessage());
            return "Error in generating and uploading report: " + e.getMessage();
        }
    }

    private ByteArrayOutputStream generateCSVReport(LambdaLogger logger) throws IOException {
        Table table = dynamoDB.getTable(DYNAMO_TABLE_NAME);
        logger.log("Fetching data from DynamoDB table: " + DYNAMO_TABLE_NAME);
        ScanSpec scanSpec = new ScanSpec();

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
             CSVPrinter csvPrinter = new CSVPrinter(outputStreamWriter, CSVFormat.DEFAULT.withHeader(
                     "Trainer First Name", "Trainer Last Name", "Monthly Training Duration"))) {

            for (Item item : table.scan(scanSpec)) {
                Integer monthlyTrainingDuration = item.getInt("MonthlyTrainingDuration");
                if (monthlyTrainingDuration != null && monthlyTrainingDuration > 0) {
                    csvPrinter.printRecord(item.getString("TrainerFirstName"),
                            item.getString("TrainerLastName"),
                            monthlyTrainingDuration);
                }
            }
            logger.log("Data fetched and CSV report generated.");
            return outputStream;
        }
    }

    private void uploadReportToS3(ByteArrayOutputStream reportStream, LambdaLogger logger) {
        String fileName = "Trainers_Trainings_summary_" + YearMonth.now().format(DateTimeFormatter.ofPattern("yyyy_MM")) + ".csv";
        byte[] reportBytes = reportStream.toByteArray();

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(reportBytes)) {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(reportBytes.length);
            PutObjectRequest putObjectRequest = new PutObjectRequest(BUCKET_NAME, fileName, inputStream, metadata);
            s3Client.putObject(putObjectRequest);
            logger.log("Report uploaded to S3 bucket: " + BUCKET_NAME + ", with file name: " + fileName);
        } catch (IOException e) {
            throw new RuntimeException("Error uploading to S3: " + e.getMessage(), e);
        }
    }
}
