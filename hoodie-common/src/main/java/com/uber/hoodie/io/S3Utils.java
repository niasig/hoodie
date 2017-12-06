/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.uber.hoodie.common.util.FSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Signifyd modification.
 */
public class S3Utils {
    private static final Logger logger = LoggerFactory.getLogger(S3Utils.class);
    private static final String EMRFS_TABLE_KEY = "fs.s3.consistent.metadata.tableName";
    private static final String EMRFS_DIR_KEY = "hashKey";
    private static final String EMRFS_FILE_KEY = "rangeKey";
    private static final Supplier<Configuration> configuration = Suppliers.memoize(() -> {
        try {
            return new Path("s3:///")
                    .getFileSystem(new Configuration())
                    .getConf();
        } catch (IOException e) {
            logger.error("configuration", e);
            return new Configuration();
        }
    });

    private static final Supplier<Boolean> isEmrfs = Suppliers.memoize(() ->
            configuration.get().getBoolean("fs.s3.consistent", false)
    );

    private static final Supplier<AmazonDynamoDB> dynamodbClient = Suppliers.memoize(() -> {

        ClientConfiguration clientConfig = PredefinedClientConfigurations.dynamoDefault()
                .withRetryPolicy(PredefinedRetryPolicies.getDynamoDBDefaultRetryPolicyWithCustomMaxRetries(100000));

        return AmazonDynamoDBClientBuilder.standard()
                .withClientConfiguration(clientConfig)
                .build();
    });

    private static final Supplier<DynamoDB> dynamoDB = Suppliers.memoize(() -> new DynamoDB(dynamodbClient.get()));

    public static void sync(Path path) {
        if (isS3(path) && isEmrfs.get()) {
            try {
                FileSystem fs = FSUtils.getFs(path);
                Set<String> emrfsFiles = emrfsList(path);
                Set<String> s3Files = new HashSet<>();
                RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, false);
                while (it.hasNext()) {
                    LocatedFileStatus fileStatus = it.next();
                    s3Files.add(fileStatus.getPath().getName());
                }

                emrfsFiles.stream().filter(file -> !s3Files.contains(file))
                        .forEach(file -> {
                            Path deletePath = new Path(path, file);
                            logger.info("sync: deleting {}", deletePath);
                            emrfsDelete(deletePath);

                        });
            } catch (Exception e) {
                logger.error("sync: {}", path, e);
            }
        }
    }

    private static Set<String> emrfsList(Path parentPath) {
        String hashKey = parentPath.toString().replaceFirst("s3:/", "");

        Condition hashKeyCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withS(hashKey));

        Map<String, Condition> conditions = ImmutableMap.of(EMRFS_DIR_KEY, hashKeyCondition);

        QueryRequest request = new QueryRequest()
                .withTableName(configuration.get().get(EMRFS_TABLE_KEY))
                .withConsistentRead(true)
                .withKeyConditions(conditions);

        QueryResult result = dynamodbClient.get().query(request);
        return ImmutableSet.copyOf(result.getItems().stream()
                .map(item -> item.get(EMRFS_FILE_KEY).getS())
                .collect(Collectors.toSet()));
    }

    public static void cleanDelete(Path path) {
        try {
            if (isS3(path)) {
                FileContext fileContext = FileContext.getFileContext(path.toUri(), configuration.get());
                fileContext.delete(path, false);
            }
        } catch (java.io.IOException e) {
            logger.error("cleanDelete: {}", path, e);
        }
    }

    public static void emrfsDelete(Path path) {
        if (isS3(path) && isEmrfs.get()) {
            final ImmutableMap<String, String> keys = ImmutableMap.of(EMRFS_DIR_KEY,
                    path.getParent().toString().replaceFirst("s3:/", ""),
                    EMRFS_FILE_KEY, path.getName().toString());
            String tableName = configuration.get().get(EMRFS_TABLE_KEY);

            DeleteItemRequest request = new DeleteItemRequest()
                    .withTableName(tableName)
                    .withKey(getDeleteItemKey(path))
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            dynamodbClient.get().deleteItem(request);
        }

    }

    private static Map<String, AttributeValue> getDeleteItemKey(Path path) {
        String hashKey = path.getParent().toString().replaceFirst("s3:/", "");
        String rangeKey = path.getName();
        Preconditions.checkArgument(!Strings.isNullOrEmpty(hashKey), "Delete: hash key cannot be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(rangeKey), "Delete: range key cannot be empty");
        Map<String, AttributeValue> deleteItemKey = ImmutableMap.of(
                "hashKey", new AttributeValue().withS(hashKey),
                "rangeKey", new AttributeValue().withS(rangeKey));
        logger.info("getDeleteItemKey: {}", deleteItemKey);
        return deleteItemKey;
    }

    private static boolean isS3(Path path) {
        return path.toString().toLowerCase().startsWith("s3:");
    }

}
