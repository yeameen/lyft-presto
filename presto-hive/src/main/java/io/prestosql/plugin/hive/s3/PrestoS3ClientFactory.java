/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.hive.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Builder;
import com.amazonaws.services.s3.AmazonS3Client;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.HiveConfig;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.Optional;

import static com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import static com.amazonaws.regions.Regions.US_EAST_1;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_ACCESS_KEY;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_CONNECT_TIMEOUT;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_CREDENTIALS_PROVIDER;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_ENDPOINT;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_MAX_ERROR_RETRIES;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_PIN_CLIENT_TO_CURRENT_REGION;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_SECRET_KEY;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_SOCKET_TIMEOUT;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_SSL_ENABLED;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_USER_AGENT_PREFIX;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_USE_INSTANCE_CREDENTIALS;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

/**
 * This factory provides AmazonS3 client required for executing S3SelectPushdown requests.
 * Normal S3 GET requests use AmazonS3 clients initialized in PrestoS3FileSystem or EMRFS.
 * The ideal state will be to merge this logic with the two file systems and get rid of this
 * factory class.
 * Please do not use the client provided by this factory for any other use cases.
 */
public class PrestoS3ClientFactory
{
    private static final String S3_SELECT_PUSHDOWN_MAX_CONNECTIONS = "hive.s3select-pushdown.max-connections";
    private static String s3UserAgentSuffix = "presto";

    @GuardedBy("this")
    private AmazonS3 s3Client;

    synchronized AmazonS3 getS3Client(Configuration config, HiveConfig hiveConfig)
    {
        if (s3Client != null) {
            return s3Client;
        }

        HiveS3Config defaults = new HiveS3Config();
        String userAgentPrefix = config.get(S3_USER_AGENT_PREFIX, defaults.getS3UserAgentPrefix());
        int maxErrorRetries = config.getInt(S3_MAX_ERROR_RETRIES, defaults.getS3MaxErrorRetries());
        boolean sslEnabled = config.getBoolean(S3_SSL_ENABLED, defaults.isS3SslEnabled());
        Duration connectTimeout = Duration.valueOf(config.get(S3_CONNECT_TIMEOUT, defaults.getS3ConnectTimeout().toString()));
        Duration socketTimeout = Duration.valueOf(config.get(S3_SOCKET_TIMEOUT, defaults.getS3SocketTimeout().toString()));
        int maxConnections = config.getInt(S3_SELECT_PUSHDOWN_MAX_CONNECTIONS, hiveConfig.getS3SelectPushdownMaxConnections());

        if (hiveConfig.isS3SelectPushdownEnabled()) {
            s3UserAgentSuffix = "presto-select";
        }

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withMaxErrorRetry(maxErrorRetries)
                .withProtocol(sslEnabled ? Protocol.HTTPS : Protocol.HTTP)
                .withConnectionTimeout(toIntExact(connectTimeout.toMillis()))
                .withSocketTimeout(toIntExact(socketTimeout.toMillis()))
                .withMaxConnections(maxConnections)
                .withUserAgentPrefix(userAgentPrefix)
                .withUserAgentSuffix(s3UserAgentSuffix);

        PrestoS3FileSystemStats stats = new PrestoS3FileSystemStats();
        RequestMetricCollector metricCollector = new PrestoS3FileSystemMetricCollector(stats);
        AWSCredentialsProvider awsCredentialsProvider = getAwsCredentialsProvider(config, defaults);
        AmazonS3Builder<? extends AmazonS3Builder<?, ?>, ? extends AmazonS3> clientBuilder = AmazonS3Client.builder()
                .withCredentials(awsCredentialsProvider)
                .withClientConfiguration(clientConfiguration)
                .withMetricsCollector(metricCollector)
                .enablePathStyleAccess();

        boolean regionOrEndpointSet = false;

        String endpoint = config.get(S3_ENDPOINT);
        boolean pinS3ClientToCurrentRegion = config.getBoolean(S3_PIN_CLIENT_TO_CURRENT_REGION, defaults.isPinS3ClientToCurrentRegion());
        verify(!pinS3ClientToCurrentRegion || endpoint == null,
                "Invalid configuration: either endpoint can be set or S3 client can be pinned to the current region");

        // use local region when running inside of EC2
        if (pinS3ClientToCurrentRegion) {
            Region region = Regions.getCurrentRegion();
            if (region != null) {
                clientBuilder.withRegion(region.getName());
                regionOrEndpointSet = true;
            }
        }

        if (!isNullOrEmpty(endpoint)) {
            clientBuilder.withEndpointConfiguration(new EndpointConfiguration(endpoint, null));
            regionOrEndpointSet = true;
        }

        if (!regionOrEndpointSet) {
            clientBuilder.withRegion(US_EAST_1);
            clientBuilder.setForceGlobalBucketAccessEnabled(true);
        }

        s3Client = clientBuilder.build();
        return s3Client;
    }

    private AWSCredentialsProvider getAwsCredentialsProvider(Configuration conf, HiveS3Config defaults)
    {
        Optional<AWSCredentials> credentials = getAwsCredentials(conf);
        if (credentials.isPresent()) {
            return new AWSStaticCredentialsProvider(credentials.get());
        }

        boolean useInstanceCredentials = conf.getBoolean(S3_USE_INSTANCE_CREDENTIALS, defaults.isS3UseInstanceCredentials());
        if (useInstanceCredentials) {
            return InstanceProfileCredentialsProvider.getInstance();
        }

        String providerClass = conf.get(S3_CREDENTIALS_PROVIDER);
        if (!isNullOrEmpty(providerClass)) {
            return getCustomAWSCredentialsProvider(conf, providerClass);
        }

        return DefaultAWSCredentialsProviderChain.getInstance();
    }

    private static AWSCredentialsProvider getCustomAWSCredentialsProvider(Configuration conf, String providerClass)
    {
        try {
            return conf.getClassByName(providerClass)
                    .asSubclass(AWSCredentialsProvider.class)
                    .getConstructor(URI.class, Configuration.class)
                    .newInstance(null, conf);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(format("Error creating an instance of %s", providerClass), e);
        }
    }

    private static Optional<AWSCredentials> getAwsCredentials(Configuration conf)
    {
        String accessKey = conf.get(S3_ACCESS_KEY);
        String secretKey = conf.get(S3_SECRET_KEY);

        if (isNullOrEmpty(accessKey) || isNullOrEmpty(secretKey)) {
            return Optional.empty();
        }
        return Optional.of(new BasicAWSCredentials(accessKey, secretKey));
    }
}
