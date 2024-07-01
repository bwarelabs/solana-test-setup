package com.bwarelabs;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedUpload;
import software.amazon.awssdk.transfer.s3.model.Upload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

public class CosUtils {
    private static final String BUCKET_NAME = "bwaresolanatest-1322657745";
    private static final String COS_ENDPOINT = "http://cos.ap-chengdu.myqcloud.com";
    private static final String REGION = "ap-chengdu";
    private static final String AWS_ID_KEY = "test";
    private static final String AWS_SECRET_KEY = "test";

    private static final S3AsyncClient s3AsyncClient = S3AsyncClient.builder()
            .endpointOverride(URI.create(COS_ENDPOINT))
            .region(Region.of(REGION))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(AWS_ID_KEY, AWS_SECRET_KEY)))
            .build();

    private static final S3TransferManager transferManager = S3TransferManager.builder()
            .s3Client(s3AsyncClient)
            .build();

    public static CompletableFuture<CompletedUpload> uploadToCos(String key, byte[] content) {
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

        return upload.completionFuture();
    }
}
