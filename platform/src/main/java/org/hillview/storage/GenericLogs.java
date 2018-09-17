/*
 * Copyright (c) 2017 MapleLabs. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
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

package org.hillview.storage;

import org.apache.commons.lang.StringEscapeUtils;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;
import org.hillview.storage.FileSetDescription;
import org.hillview.utils.Utilities;

import java.util.Map;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import io.krakens.grok.api.*;

/**
 * Reads Generic logs into ITable objects.
 */

public class GenericLogs {
    private static final Schema schema = new Schema();
    private static String schema_defined = "no";
    private static int schema_columns = 0;
    private static Grok grok;
    private static String logFormat;

    GenericLogs(String logFormat) {
        this.logFormat = logFormat;
        GrokCompiler grokCompiler = GrokCompiler.newInstance();
        grokCompiler.registerDefaultPatterns();
        grokCompiler.registerPatternFromClasspath("/patterns/log-patterns");
        grok = grokCompiler.compile(this.logFormat, true);
    }

    static {
        GenericLogs.schema.append(new ColumnDescription("Host", ContentsKind.String));
    }

    public static class LogFileLoader extends TextFileLoader {
        LogFileLoader(final String path) {
            super(path);
        }

        void parse(String line, String[] output) {
            Match gm = grok.match(line);
            final Map<String, Object> capture = gm.capture();
            if (capture.size() > 0) {
                int index = 1;
                for (Map.Entry<String,Object> entry : capture.entrySet()) {
                    if (schema_defined.equals("no"))
                        GenericLogs.schema.append(new ColumnDescription(entry.getKey(), ContentsKind.String));
                    output[0] = Utilities.getHostName();
                    if (entry.getKey().toLowerCase().contains("timestamp"))
                        output[index] = entry.getValue().toString().replace(",", ".");
                    else
                        output[index] = entry.getValue().toString();
                    index += 1;
		}
                schema_defined = "yes";
            }
        }

        @Override
        public ITable load() {
            this.columns = schema.createAppendableColumns();
            try (BufferedReader reader = new BufferedReader(
                    new FileReader(this.filename))) {
                if (schema_columns == 0) {
                    String line = reader.readLine();
                    Match gm = grok.match(line);
                    final Map<String, Object> capture = gm.capture();
                    schema_columns = capture.size();
                }
                String[] fields = new String[schema_columns + 1];
                while (true) {
                    String line = reader.readLine();
                    if (line == null)
                        break;
                    if (line.trim().isEmpty())
                        continue;
                    this.parse(line, fields);
                    this.append(fields);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            this.close(null);
            return new Table(this.columns, this.filename, null);
        }
    }

    public static ITable parseLogFile(String file) {
        LogFileLoader reader = new LogFileLoader(file);
        return reader.load();
    }
}
