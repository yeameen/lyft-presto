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

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Locale.ENGLISH;
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

    public Set<String> getTableNames()
    {
        return sheetsDataProvider.listTables();
    }

    public Optional<SheetsTable> getTable(String tableName)
    {
        List<List<Object>> values = sheetsDataProvider.readAllValues(tableName);
        if (values.size() > 0) {
            ImmutableList.Builder<SheetsColumn> columns = ImmutableList.builder();
            Set<String> columnNames = new HashSet<>();
            // Assuming 1st line is always header
            List<Object> header = values.get(0);
            int count = 0;
            for (Object column : header) {
                String columnValue = column.toString().toLowerCase(ENGLISH);
                // when empty or repeated column header, adding a placeholder column name
                if (columnValue.isEmpty() || columnNames.contains(columnValue)) {
                    columnValue = "column_" + ++count;
                }
                columnNames.add(columnValue);
                columns.add(new SheetsColumn(columnValue, VarcharType.VARCHAR));
            }
            List<List<Object>> dataValues = values.subList(1, values.size()); // removing header info
            return Optional.of(new SheetsTable(tableName, columns.build(), dataValues));
        }
        return Optional.empty();
    }
}
