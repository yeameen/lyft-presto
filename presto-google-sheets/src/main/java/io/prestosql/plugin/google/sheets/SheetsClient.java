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

import io.airlift.json.JsonCodec;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.prestosql.plugin.google.sheets.SheetsErrorCode.SHEETS_UNKNOWN_TABLE_ERROR;
import static java.util.Objects.requireNonNull;

public class SheetsClient
{
    private final GSheetsDataProvider sheetsDataProvider;

    @Inject
    public SheetsClient(SheetsConfig config, JsonCodec<Map<String, List<SheetsTable>>> catalogCodec, GSheetsDataProvider sheetsDataProvider)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");
        requireNonNull(sheetsDataProvider, "sheetsDataProvider is null");
        this.sheetsDataProvider = sheetsDataProvider;
    }

    public Set<String> getTableNames(String schema)
    {
        return sheetsDataProvider.listTables();
    }

    public Optional<SheetsTable> getTable(String schema, String tableName)
    {
        try {
            Optional<List<List<Object>>> values = sheetsDataProvider.readAllValues(tableName);
            if (values.isPresent() && values.get().size() > 0) {
                List<SheetsColumn> columns = new ArrayList<>();
                Set<String> columnNames = new HashSet<>();
                // Assuming 1st line is always header
                List<Object> header = values.get().get(0);
                int count = 0;
                for (Object col : header) {
                    String colVal = String.valueOf(col);
                    // when empty or repeated column header, adding a placeholder column name
                    if ("".equals(colVal) || columnNames.contains(colVal)) {
                        colVal = "col_" + ++count;
                    }
                    columnNames.add(colVal);
                    columns.add(new SheetsColumn(colVal, VarcharType.VARCHAR));
                }
                List<List<Object>> dataValues = values.get().subList(1, values.get().size()); // removing header info
                return Optional.of(new SheetsTable(tableName, columns, dataValues));
            }
        }
        catch (ExecutionException e) {
            throw new PrestoException(SHEETS_UNKNOWN_TABLE_ERROR, e);
        }
        return Optional.empty();
    }
}
