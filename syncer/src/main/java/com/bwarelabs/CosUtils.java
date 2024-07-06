package com.bwarelabs;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedUpload;
import software.amazon.awssdk.transfer.s3.model.Upload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class CosUtils {
    private static final Logger logger = Logger.getLogger(CosUtils.class.getName());

    private static final String BUCKET_NAME = "bwaresolanatest-1322657745";
    private static final String COS_ENDPOINT = "http://cos.ap-chengdu.myqcloud.com";
    private static final String REGION = "ap-chengdu";
    private static final String AWS_ID_KEY = "test";
    private static final String AWS_SECRET_KEY = "test";

    private static final SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient
            .builder()
            .maxConcurrency(200)
            .connectionAcquisitionTimeout(Duration.ofSeconds(60))
            .build();

    private static final S3AsyncClient s3AsyncClient = S3AsyncClient.builder()
            .httpClient(httpClient)
            .endpointOverride(URI.create(COS_ENDPOINT))
            .region(Region.of(REGION))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(AWS_ID_KEY, AWS_SECRET_KEY)))
            .build();

    private static final S3TransferManager transferManager = S3TransferManager.builder()
            .s3Client(s3AsyncClient)
            .build();

    public static CompletableFuture<CompletedUpload> uploadToCos(String key, byte[] content) {
        if (key == null || key.trim().isEmpty()) {
            logger.severe("Key cannot be null or empty");
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        if (content == null || content.length == 0) {
            logger.severe("Content cannot be null or empty");
            throw new IllegalArgumentException("Content cannot be null or empty");
        }

        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(key)
                    .build();

            UploadRequest uploadRequest = UploadRequest.builder()
                    .putObjectRequest(putObjectRequest)
                    .requestBody(AsyncRequestBody.fromBytes(content))
                    .addTransferListener(LoggingTransferListener.create())
                    .build();

            Upload upload = transferManager.upload(uploadRequest);

            return upload.completionFuture().exceptionally(ex -> {
                throw new RuntimeException("Upload to COS failed", ex);
            });
        } catch (Exception e) {
            throw new RuntimeException("Error initiating upload to COS", e);
        }
    }
}