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
package io.prestosql.plugin.google.sheets;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;
import org.apache.http.annotation.ThreadSafe;

import javax.inject.Inject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.api.client.googleapis.javanet.GoogleNetHttpTransport.newTrustedTransport;
import static com.google.common.cache.CacheLoader.from;
import static io.prestosql.plugin.google.sheets.SheetsErrorCode.SHEETS_BAD_CREDENTIALS_ERROR;
import static io.prestosql.plugin.google.sheets.SheetsErrorCode.SHEETS_METASTORE_ERROR;
import static io.prestosql.plugin.google.sheets.SheetsErrorCode.SHEETS_UNKNOWN_TABLE_ERROR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class GSheetsDataProvider
{
    private static final Logger log = Logger.get(GSheetsDataProvider.class);
    private static final String APPLICATION_NAME = "presto google sheets integration";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    private static final List<String> SCOPES = ImmutableList.of(SheetsScopes.SPREADSHEETS_READONLY);

    private final LoadingCache<String, Optional<String>> tableSheetMappingCache;
    private final LoadingCache<String, Optional<List<List<Object>>>> sheetDataCache;

    private final String metadataSheetId;
    private final String credentialsFilePath;

    private final Sheets sheetsService;

    @Inject
    public GSheetsDataProvider(SheetsConfig config)
    {
        this.metadataSheetId = config.getMetadataSheetId();
        this.credentialsFilePath = config.getCredentialsFilePath();

        try {
            this.sheetsService = new Sheets.Builder(newTrustedTransport(), JSON_FACTORY, getCredentials()).setApplicationName(APPLICATION_NAME).build();
        }
        catch (Exception e) {
            throw new PrestoException(SHEETS_BAD_CREDENTIALS_ERROR, e);
        }
        long expiresAfterWriteMillis = config.getSheetsDataExpireAfterWrite().toMillis();
        long maxCacheSize = config.getSheetsDataMaxCacheSize();

        this.tableSheetMappingCache = newCacheBuilder(expiresAfterWriteMillis, maxCacheSize)
                .build(new CacheLoader<String, Optional<String>>() {
                    @Override
                    public Optional<String> load(String tableName)
                    {
                        return getSheetExpressionForTable(tableName);
                    }

                    @Override
                    public Map<String, Optional<String>> loadAll(Iterable<? extends String> tableList)
                    {
                        return getSheetExpressionForAllTables();
                    }
                });

        this.sheetDataCache = newCacheBuilder(expiresAfterWriteMillis, maxCacheSize).build(from(this::getDataForSheetExpression));
    }

    private Optional<String> getSheetExpressionForTable(String tableName)
    {
        try {
            List<List<Object>> data = readAllValuesFromSheetExpression(metadataSheetId);
            // first line is assumed to be sheet header
            for (int i = 1; i < data.size(); i++) {
                if (data.get(i).size() >= 2) {
                    String tableId = String.valueOf(data.get(i).get(0));
                    String sheetId = String.valueOf(data.get(i).get(1));
                    if (tableName.equalsIgnoreCase(tableId)) {
                        return Optional.of(sheetId);
                    }
                }
            }
        }
        catch (IOException e) {
            throw new PrestoException(SHEETS_METASTORE_ERROR, e);
        }
        return Optional.empty();
    }

    private Map<String, Optional<String>> getSheetExpressionForAllTables()
    {
        ImmutableMap.Builder<String, Optional<String>> tableSheetMap = ImmutableMap.builder();
        try {
            List<List<Object>> data = readAllValuesFromSheetExpression(metadataSheetId);
            // first line is assumed to be sheet header
            for (int i = 1; i < data.size(); i++) {
                if (data.get(i).size() >= 2) {
                    String tableId = String.valueOf(data.get(i).get(0));
                    String sheetId = String.valueOf(data.get(i).get(1));
                    tableSheetMap.put(tableId, Optional.of(sheetId));
                }
            }
        }
        catch (IOException e) {
            throw new PrestoException(SHEETS_METASTORE_ERROR, e);
        }
        return tableSheetMap.build();
    }

    private Optional<List<List<Object>>> getDataForSheetExpression(String sheetExpression)
    {
        try {
            return Optional.of(readAllValuesFromSheetExpression(sheetExpression));
        }
        catch (IOException e) {
            throw new PrestoException(SHEETS_UNKNOWN_TABLE_ERROR, e);
        }
    }

    private Credential getCredentials()
    {
        GoogleCredential credential;
        try (InputStream in = new FileInputStream(credentialsFilePath)) {
            credential = GoogleCredential.fromStream(in).createScoped(SCOPES);
        }
        catch (IOException e) {
            throw new PrestoException(SHEETS_BAD_CREDENTIALS_ERROR, e);
        }
        return credential;
    }

    public Set<String> listTables()
    {
        Set<String> tables = new HashSet<>();
        try {
            Optional<List<List<Object>>> tableMetadata = sheetDataCache.get(metadataSheetId);
            if (tableMetadata.isPresent()) {
                for (int i = 1; i < tableMetadata.get().size(); i++) {
                    if (tableMetadata.get().get(i).size() > 0) {
                        tables.add(String.valueOf(tableMetadata.get().get(i).get(0)));
                    }
                }
            }
        }
        catch (ExecutionException e) {
            throw new PrestoException(SHEETS_METASTORE_ERROR, e);
        }
        return tables;
    }

    public Optional<List<List<Object>>> readAllValues(String tableName) throws ExecutionException
    {
        Optional<String> sheetExpression = tableSheetMappingCache.get(tableName);
        if (!sheetExpression.isPresent()) {
            throw new PrestoException(SHEETS_UNKNOWN_TABLE_ERROR, "Data not found for table " + tableName);
        }
        return sheetDataCache.get(sheetExpression.get());
    }

    private List<List<Object>> readAllValuesFromSheetExpression(String sheetExpression) throws IOException
    {
        // by default loading up to max 10k columns
        String defaultRange = "$1:$10000";
        String[] tableOptions = sheetExpression.split("#");
        String sheetId = tableOptions[0];
        if (tableOptions.length > 1) {
            defaultRange = tableOptions[1];
        }
        log.info("Accessing sheet id [%s] with range [%s]", sheetId, defaultRange);
        return sheetsService.spreadsheets().values().get(sheetId, defaultRange).execute().getValues();
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(long expiresAfterWriteMillis, long maximumSize)
    {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        cacheBuilder = cacheBuilder.expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS);
        cacheBuilder = cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }
}
