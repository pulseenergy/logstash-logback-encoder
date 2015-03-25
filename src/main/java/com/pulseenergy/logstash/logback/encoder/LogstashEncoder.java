/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pulseenergy.logstash.logback.encoder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Marker;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.encoder.EncoderBase;
import ch.qos.logback.core.util.CachingDateFormatter;

import com.grack.nanojson.JsonAppendableWriter;
import com.grack.nanojson.JsonWriter;

public class LogstashEncoder extends EncoderBase<ILoggingEvent> {
    
    private static final CachingDateFormatter ISO_DATETIME_TIME_ZONE_FORMAT_WITH_MILLIS = new CachingDateFormatter("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
    private static final StackTraceElement DEFAULT_CALLER_DATA = new StackTraceElement("", "", "", 0);
    
    private boolean immediateFlush = true;

    /**
     * If true, the caller information is included in the logged data.
     * Note: calculating the caller data is an expensive operation.
     */
    private boolean includeCallerInfo = true;

    private boolean includeTags = false;

    @Override
    public void doEncode(ILoggingEvent event) throws IOException {

        JsonAppendableWriter writer = JsonWriter.on(outputStream);

        writer.object();
        writer.value("@timestamp", ISO_DATETIME_TIME_ZONE_FORMAT_WITH_MILLIS.format(event.getTimeStamp()));
        writer.value("@message", event.getFormattedMessage());
        writer.object("@fields");
        writeFields(writer, event);
        writer.end();

        if (includeTags) {
            writer.array("@tags");
            writeTags(writer, event);
            writer.end();
        }
        writer.end();
        writer.done();

        outputStream.write(CoreConstants.LINE_SEPARATOR.getBytes(Charset.defaultCharset()));

        if (immediateFlush) {
            outputStream.flush();
        }

    }

    private void writeFields(JsonAppendableWriter writer, ILoggingEvent event) throws IOException {

        writer.value("logger_name", event.getLoggerName());
        writer.value("thread_name", event.getThreadName());
        writer.value("level", event.getLevel().toString());
        writer.value("level_value", event.getLevel().toInt());

        if (includeCallerInfo) {
            StackTraceElement callerData = extractCallerData(event);
            writer.value("caller_class_name", callerData.getClassName());
            writer.value("caller_method_name", callerData.getMethodName());
            writer.value("caller_file_name", callerData.getFileName());
            writer.value("caller_line_number", callerData.getLineNumber());
        }

        IThrowableProxy throwableProxy = event.getThrowableProxy();
        if (throwableProxy != null) {
            writer.value("stack_trace", ThrowableProxyUtil.asString(throwableProxy));
        }

        Context context = getContext();
        if (context != null) {
            writePropertiesAsFields(writer, context.getCopyOfPropertyMap());
        }
        writePropertiesAsFields(writer, event.getMDCPropertyMap());

    }
    
    private void writeTags(JsonAppendableWriter writer, ILoggingEvent event) throws IOException {
        final Marker marker = event.getMarker();

        if (marker != null) {
            writer.value(marker.getName());

            if (marker.hasReferences()) {
                final Iterator<?> i = event.getMarker().iterator();
                
                while (i.hasNext()) {
                    Marker next = (Marker) i.next();
                    
                    // attached markers will never be null as provided by the MarkerFactory.
                    writer.value(next.getName());
                }
            }
        }
    }
    
    private void writePropertiesAsFields(final JsonAppendableWriter writer, final Map<String, String> properties) throws IOException {
        if (properties != null) {
            for (Entry<String, String> entry : properties.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                writer.value(key, value);
            }
        }
    }
    
    private StackTraceElement extractCallerData(final ILoggingEvent event) {
        final StackTraceElement[] ste = event.getCallerData();
        if (ste == null || ste.length == 0) {
            return DEFAULT_CALLER_DATA;
        }
        return ste[0];
    }
    
    @Override
    public void close() throws IOException {
        outputStream.write(CoreConstants.LINE_SEPARATOR.getBytes(Charset.defaultCharset()));
    }

    public boolean isImmediateFlush() {
        return immediateFlush;
    }
    
    public void setImmediateFlush(boolean immediateFlush) {
        this.immediateFlush = immediateFlush;
    }
    
    public boolean isIncludeCallerInfo() {
        return includeCallerInfo;
    }
    
    public void setIncludeCallerInfo(boolean includeCallerInfo) {
        this.includeCallerInfo = includeCallerInfo;
    }

    public boolean isIncludeTags() {
        return includeTags;
    }

    public void setIncludeTags(boolean includeTags) {
        this.includeTags = includeTags;
    }
}
