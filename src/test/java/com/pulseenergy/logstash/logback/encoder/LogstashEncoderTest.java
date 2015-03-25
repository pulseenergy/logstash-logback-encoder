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

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import ch.qos.logback.core.CoreConstants;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.Context;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LogstashEncoderTest {
    
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private LogstashEncoder encoder;
    private ByteArrayOutputStream outputStream;
    
    @Before
    public void before() throws Exception {
        outputStream = new ByteArrayOutputStream();
        encoder = new LogstashEncoder();
        encoder.init(outputStream);
    }
    
    @Test
    public void basicsAreIncluded() throws Exception {
        final long timestamp = System.currentTimeMillis();
        
        ILoggingEvent event = mockBasicILoggingEvent(Level.ERROR);
        when(event.getTimeStamp()).thenReturn(timestamp);
        
        encoder.doEncode(event);
        outputStream.close();

        JsonNode node = MAPPER.readTree(outputStream.toByteArray());
        
        assertThat(node.get("@timestamp").textValue(), is(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ").format(timestamp)));
        assertThat(node.get("@fields").get("logger_name").textValue(), is("LoggerName"));
        assertThat(node.get("@fields").get("thread_name").textValue(), is("ThreadName"));
        assertThat(node.get("@message").textValue(), is("My message"));
        assertThat(node.get("@fields").get("level").textValue(), is("ERROR"));
        assertThat(node.get("@fields").get("level_value").intValue(), is(40000));
    }
    
    @Test
    public void closePutsSeparatorAtTheEnd() throws Exception {
        ILoggingEvent event = mockBasicILoggingEvent(Level.ERROR);
        
        encoder.doEncode(event);
        encoder.close();
        outputStream.close();

        assertThat(outputStream.toString(), Matchers.endsWith(CoreConstants.LINE_SEPARATOR));
    }
    
    @Test
    public void includingThrowableProxyIncludesStackTrace() throws Exception {
        IThrowableProxy throwableProxy = new ThrowableProxy(new Exception("My goodness"));
        
        ILoggingEvent event = mockBasicILoggingEvent(Level.ERROR);
        when(event.getThrowableProxy()).thenReturn(throwableProxy);
        
        encoder.doEncode(event);
        outputStream.close();

        JsonNode node = MAPPER.readTree(outputStream.toByteArray());
        
        assertThat(node.get("@fields").get("stack_trace").textValue(), is(ThrowableProxyUtil.asString(throwableProxy)));
    }
    
    @Test
    public void propertiesInMDCAreIncluded() throws Exception {
        Map<String, String> mdcMap = new HashMap<String, String>();
        mdcMap.put("thing_one", "One");
        mdcMap.put("thing_two", "Three");
        
        ILoggingEvent event = mockBasicILoggingEvent(Level.ERROR);
        when(event.getMDCPropertyMap()).thenReturn(mdcMap);
        
        encoder.doEncode(event);
        outputStream.close();

        JsonNode node = MAPPER.readTree(outputStream.toByteArray());
        
        assertThat(node.get("@fields").get("thing_one").textValue(), is("One"));
        assertThat(node.get("@fields").get("thing_two").textValue(), is("Three"));
    }
    
    @Test
    public void nullMDCDoesNotCauseEverythingToBlowUp() throws Exception {
        ILoggingEvent event = mockBasicILoggingEvent(Level.ERROR);
        when(event.getMDCPropertyMap()).thenReturn(null);
        
        encoder.doEncode(event);
        outputStream.close();
    }
    
    @Test
    public void callerDataIsIncluded() throws Exception {
        ILoggingEvent event = mockBasicILoggingEvent(Level.ERROR);
        when(event.getMDCPropertyMap()).thenReturn(Collections.<String, String> emptyMap());
        final StackTraceElement[] stackTraceElements = { new StackTraceElement("caller_class", "method_name", "file_name", 12345) };
        when(event.getCallerData()).thenReturn(stackTraceElements);
        
        encoder.setIncludeCallerInfo(true);
        
        encoder.doEncode(event);
        outputStream.close();

        JsonNode node = MAPPER.readTree(outputStream.toByteArray());
        
        assertThat(node.get("@fields").get("caller_class_name").textValue(), is(stackTraceElements[0].getClassName()));
        assertThat(node.get("@fields").get("caller_method_name").textValue(), is(stackTraceElements[0].getMethodName()));
        assertThat(node.get("@fields").get("caller_file_name").textValue(), is(stackTraceElements[0].getFileName()));
        assertThat(node.get("@fields").get("caller_line_number").intValue(), is(stackTraceElements[0].getLineNumber()));
    }
    
    @Test
    public void callerDataIsNotIncludedIfSwitchedOff() throws Exception {
        ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLoggerName()).thenReturn("LoggerName");
        when(event.getThreadName()).thenReturn("ThreadName");
        when(event.getFormattedMessage()).thenReturn("My message");
        when(event.getLevel()).thenReturn(Level.ERROR);
        when(event.getMDCPropertyMap()).thenReturn(Collections.<String, String> emptyMap());
        final StackTraceElement[] stackTraceElements = { new StackTraceElement("caller_class", "method_name", "file_name", 12345) };
        when(event.getCallerData()).thenReturn(stackTraceElements);
        
        encoder.setIncludeCallerInfo(false);
        
        encoder.doEncode(event);
        outputStream.close();

        JsonNode node = MAPPER.readTree(outputStream.toByteArray());
        assertThat(node.get("@fields").get("caller_class_name"), is(nullValue()));
        assertThat(node.get("@fields").get("caller_method_name"), is(nullValue()));
        assertThat(node.get("@fields").get("caller_file_name"), is(nullValue()));
        assertThat(node.get("@fields").get("caller_line_number"), is(nullValue()));
    }
    
    @Test
    public void propertiesInContextAreIncluded() throws Exception {
        Map<String, String> propertyMap = new HashMap<String, String>();
        propertyMap.put("thing_one", "One");
        propertyMap.put("thing_two", "Three");
        
        final Context context = mock(Context.class);
        when(context.getCopyOfPropertyMap()).thenReturn(propertyMap);
        
        ILoggingEvent event = mockBasicILoggingEvent(Level.ERROR);
        
        encoder.setContext(context);
        encoder.doEncode(event);
        outputStream.close();

        JsonNode node = MAPPER.readTree(outputStream.toByteArray());
        
        assertThat(node.get("@fields").get("thing_one").textValue(), is("One"));
        assertThat(node.get("@fields").get("thing_two").textValue(), is("Three"));
    }
    
    @Test
    public void markerIncludesItselfAsTag() throws Exception {
        Marker marker = MarkerFactory.getMarker("hoosh");
        ILoggingEvent event = mockBasicILoggingEvent(Level.INFO);
        when(event.getMarker()).thenReturn(marker);

        encoder.setIncludeTags(true);
        
        encoder.doEncode(event);
        outputStream.close();

        JsonNode node = MAPPER.readTree(outputStream.toByteArray());
        
        assertJsonArray(node.findValue("@tags"), "hoosh");
    }
    
    @Test
    public void markerReferencesAreIncludedAsTags() throws Exception {
        Marker marker = MarkerFactory.getMarker("bees");
        marker.add(MarkerFactory.getMarker("knees"));
        ILoggingEvent event = mockBasicILoggingEvent(Level.INFO);
        when(event.getMarker()).thenReturn(marker);

        encoder.setIncludeTags(true);

        encoder.doEncode(event);
        outputStream.close();

        JsonNode node = MAPPER.readTree(outputStream.toByteArray());
        
        assertJsonArray(node.findValue("@tags"), "bees", "knees");
    }
    
    @Test
    public void nullMarkerIsIgnored() throws Exception {
        ILoggingEvent event = mockBasicILoggingEvent(Level.INFO);
        when(event.getMarker()).thenReturn(null);

        encoder.setIncludeTags(true);

        encoder.doEncode(event);
        outputStream.close();

        JsonNode node = MAPPER.readTree(outputStream.toByteArray());
        
        assertJsonArray(node.findValue("@tags"));
    }
    
    @Test
    public void immediateFlushIsSane() {
        encoder.setImmediateFlush(true);
        assertThat(encoder.isImmediateFlush(), is(true));
        
        encoder.setImmediateFlush(false);
        assertThat(encoder.isImmediateFlush(), is(false));
    }
    
    private void assertJsonArray(JsonNode jsonNode, String... expected) {
        String[] values = new String[jsonNode.size()];
        for (int i = 0; i < values.length; i++) {
            values[i] = jsonNode.get(i).asText();
        }
        
        Assert.assertArrayEquals(expected, values);
    }
    
    private ILoggingEvent mockBasicILoggingEvent(Level level) {
        ILoggingEvent event = mock(ILoggingEvent.class);
        when(event.getLoggerName()).thenReturn("LoggerName");
        when(event.getThreadName()).thenReturn("ThreadName");
        when(event.getFormattedMessage()).thenReturn("My message");
        when(event.getLevel()).thenReturn(level);
        return event;
    }

}
