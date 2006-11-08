/*
 * $HeadURL$
 * $Revision$
 * $Date$
 * ====================================================================
 *
 *  Copyright 1999-2006 The Apache Software Foundation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 *
 */

package org.apache.http.nio.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.http.impl.DefaultHttpParams;
import org.apache.http.io.CharArrayBuffer;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;

/**
 * Simple tests for {@link SessionInputBuffer} and {@link SessionOutputBuffer}.
 *
 * @author <a href="mailto:oleg at ural.ru">Oleg Kalnichevski</a>
 * 
 * @version $Id$
 */
public class TestSessionInOutBuffers extends TestCase {

    // ------------------------------------------------------------ Constructor
    public TestSessionInOutBuffers(String testName) {
        super(testName);
    }

    // ------------------------------------------------------------------- Main
    public static void main(String args[]) {
        String[] testCaseName = { TestSessionInOutBuffers.class.getName() };
        junit.textui.TestRunner.main(testCaseName);
    }

    // ------------------------------------------------------- TestCase Methods

    public static Test suite() {
        return new TestSuite(TestSessionInOutBuffers.class);
    }

    private static WritableByteChannel newChannel(final ByteArrayOutputStream outstream) {
        return Channels.newChannel(outstream);
    }
    
    private static ReadableByteChannel newChannel(final byte[] bytes) { 
        return Channels.newChannel(new ByteArrayInputStream(bytes));
    }

    private static ReadableByteChannel newChannel(final String s, final String charset) 
            throws UnsupportedEncodingException {
        return Channels.newChannel(new ByteArrayInputStream(s.getBytes(charset)));
    }

    private static ReadableByteChannel newChannel(final String s) 
            throws UnsupportedEncodingException {
        return newChannel(s, "US-ASCII");
    }

    public void testReadLineChunks() throws Exception {
        
        SessionInputBuffer inbuf = new SessionInputBuffer(16, 16);
        
        ReadableByteChannel channel = newChannel("One\r\nTwo\r\nThree");
        
        inbuf.fill(channel);
        
        CharArrayBuffer line = new CharArrayBuffer(64);
        
        line.clear();
        assertTrue(inbuf.readLine(line, false));
        assertEquals("One", line.toString());
        
        line.clear();
        assertTrue(inbuf.readLine(line, false));
        assertEquals("Two", line.toString());

        line.clear();
        assertFalse(inbuf.readLine(line, false));

        channel = newChannel("\r\nFour");
        inbuf.fill(channel);
        
        line.clear();
        assertTrue(inbuf.readLine(line, false));
        assertEquals("Three", line.toString());

        inbuf.fill(channel);
        
        line.clear();
        assertTrue(inbuf.readLine(line, true));
        assertEquals("Four", line.toString());

        line.clear();
        assertFalse(inbuf.readLine(line, true));
    }
    
    public void testWriteLineChunks() throws Exception {
        
        SessionOutputBuffer outbuf = new SessionOutputBuffer(16, 16);
        SessionInputBuffer inbuf = new SessionInputBuffer(16, 16);
        
        ReadableByteChannel inChannel = newChannel("One\r\nTwo\r\nThree");
        
        inbuf.fill(inChannel);
        
        CharArrayBuffer line = new CharArrayBuffer(64);
        
        line.clear();
        assertTrue(inbuf.readLine(line, false));
        assertEquals("One", line.toString());
        
        outbuf.writeLine(line);
        
        line.clear();
        assertTrue(inbuf.readLine(line, false));
        assertEquals("Two", line.toString());

        outbuf.writeLine(line);
        
        line.clear();
        assertFalse(inbuf.readLine(line, false));

        inChannel = newChannel("\r\nFour");
        inbuf.fill(inChannel);

        line.clear();
        assertTrue(inbuf.readLine(line, false));
        assertEquals("Three", line.toString());

        outbuf.writeLine(line);
        
        inbuf.fill(inChannel);
        
        line.clear();
        assertTrue(inbuf.readLine(line, true));
        assertEquals("Four", line.toString());

        outbuf.writeLine(line);
        
        line.clear();
        assertFalse(inbuf.readLine(line, true));

        ByteArrayOutputStream outstream = new ByteArrayOutputStream(); 
        WritableByteChannel outChannel = newChannel(outstream);
        outbuf.flush(outChannel);
        
        String s = new String(outstream.toByteArray(), "US-ASCII");
        assertEquals("One\r\nTwo\r\nThree\r\nFour\r\n", s);
    }
    
    public void testBasicReadWriteLine() throws Exception {
        
        String[] teststrs = new String[5];
        teststrs[0] = "Hello";
        teststrs[1] = "This string should be much longer than the size of the line buffer " +
                "which is only 16 bytes for this test";
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 15; i++) {
            buffer.append("123456789 ");
        }
        buffer.append("and stuff like that");
        teststrs[2] = buffer.toString();
        teststrs[3] = "";
        teststrs[4] = "And goodbye";
        
        SessionOutputBuffer outbuf = new SessionOutputBuffer(1024, 16); 
        for (int i = 0; i < teststrs.length; i++) {
            outbuf.writeLine(teststrs[i]);
        }
        //this write operation should have no effect
        outbuf.writeLine((String)null);
        outbuf.writeLine((CharArrayBuffer)null);
        
        ByteArrayOutputStream outstream = new ByteArrayOutputStream(); 
        WritableByteChannel outChannel = newChannel(outstream);
        outbuf.flush(outChannel);

        ReadableByteChannel channel = newChannel(outstream.toByteArray());
        
        SessionInputBuffer inbuf = new SessionInputBuffer(1024, 16);
        inbuf.fill(channel);
        
        for (int i = 0; i < teststrs.length; i++) {
            assertEquals(teststrs[i], inbuf.readLine(true));
        }
        assertNull(inbuf.readLine(true));
        assertNull(inbuf.readLine(true));
    }

    public void testComplexReadWriteLine() throws Exception {
        SessionOutputBuffer outbuf = new SessionOutputBuffer(1024, 16); 
        outbuf.write(new byte[] {'a', '\n'});
        outbuf.write(new byte[] {'\r', '\n'});
        outbuf.write(new byte[] {'\r', '\r', '\n'});
        outbuf.write(new byte[] {'\n'});
        //these write operations should have no effect
        outbuf.write(null, 0, 12);
        
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 14; i++) {
            buffer.append("a");
        }
        String s1 = buffer.toString();
        buffer.append("\r\n");
        outbuf.write(buffer.toString().getBytes("US-ASCII"));

        buffer.setLength(0);
        for (int i = 0; i < 15; i++) {
            buffer.append("a");
        }
        String s2 = buffer.toString();
        buffer.append("\r\n");
        outbuf.write(buffer.toString().getBytes("US-ASCII"));

        buffer.setLength(0);
        for (int i = 0; i < 16; i++) {
            buffer.append("a");
        }
        String s3 = buffer.toString();
        buffer.append("\r\n");
        outbuf.write(buffer.toString().getBytes("US-ASCII"));

        outbuf.write(new byte[] {'a'});
        
        ByteArrayOutputStream outstream = new ByteArrayOutputStream(); 
        WritableByteChannel outChannel = newChannel(outstream);
        outbuf.flush(outChannel);

        ReadableByteChannel channel = newChannel(outstream.toByteArray());

        SessionInputBuffer inbuf = new SessionInputBuffer(1024, 16);
        inbuf.fill(channel);
        
        assertEquals("a", inbuf.readLine(true));
        assertEquals("", inbuf.readLine(true));
        assertEquals("\r", inbuf.readLine(true));
        assertEquals("", inbuf.readLine(true));
        assertEquals(s1, inbuf.readLine(true));
        assertEquals(s2, inbuf.readLine(true));
        assertEquals(s3, inbuf.readLine(true));
        assertEquals("a", inbuf.readLine(true));
        assertNull(inbuf.readLine(true));
        assertNull(inbuf.readLine(true));
    }
    
    public void testReadWriteBytes() throws Exception {
        // make the buffer larger than that of transmitter
        byte[] out = new byte[40];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte)('0' + i);
        }
        SessionOutputBuffer outbuf = new SessionOutputBuffer(16, 16); 
        int off = 0;
        int remaining = out.length;
        while (remaining > 0) {
            int chunk = 10;
            if (chunk > remaining) {
                chunk = remaining;
            }
            outbuf.write(out, off, chunk);
            off += chunk;
            remaining -= chunk;
        }
        
        ByteArrayOutputStream outstream = new ByteArrayOutputStream(); 
        WritableByteChannel outChannel = newChannel(outstream);
        outbuf.flush(outChannel);

        byte[] tmp = outstream.toByteArray();
        assertEquals(out.length, tmp.length);
        for (int i = 0; i < out.length; i++) {
            assertEquals(out[i], tmp[i]);
        }
        
        ReadableByteChannel channel = newChannel(tmp);        
        SessionInputBuffer inbuf = new SessionInputBuffer(1024, 16);
        while (inbuf.fill(channel) > 0) {
        }

        // these read operations will have no effect
        assertEquals(0, inbuf.read(null, 0, 10));
        
        byte[] in = new byte[40];
        off = 0;
        remaining = in.length;
        while (remaining > 0) {
            int chunk = 10;
            if (chunk > remaining) {
                chunk = remaining;
            }
            int l = inbuf.read(in, off, chunk);
            if (l == -1) {
                break;
            }
            off += l;
            remaining -= l;
        }
        for (int i = 0; i < out.length; i++) {
            assertEquals(out[i], in[i]);
        }
        assertEquals(0, inbuf.read(tmp));
    }
    
    public void testReadWriteByte() throws Exception {
        // make the buffer larger than that of transmitter
        byte[] out = new byte[40];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte)('0' + i);
        }
        SessionOutputBuffer outbuf = new SessionOutputBuffer(16, 16); 
        for (int i = 0; i < out.length; i++) {
            outbuf.write(out[i]);
        }

        ByteArrayOutputStream outstream = new ByteArrayOutputStream(); 
        WritableByteChannel outChannel = newChannel(outstream);
        outbuf.flush(outChannel);

        byte[] tmp = outstream.toByteArray();
        assertEquals(out.length, tmp.length);
        for (int i = 0; i < out.length; i++) {
            assertEquals(out[i], tmp[i]);
        }
        
        ReadableByteChannel channel = newChannel(tmp);        
        SessionInputBuffer inbuf = new SessionInputBuffer(16, 16);
        while (inbuf.fill(channel) > 0) {
        }

        byte[] in = new byte[40];
        for (int i = 0; i < in.length; i++) {
            in[i] = (byte)inbuf.read();
        }
        for (int i = 0; i < out.length; i++) {
            assertEquals(out[i], in[i]);
        }
    }
    
    static final int SWISS_GERMAN_HELLO [] = {
        0x47, 0x72, 0xFC, 0x65, 0x7A, 0x69, 0x5F, 0x7A, 0xE4, 0x6D, 0xE4
    };
        
    static final int RUSSIAN_HELLO [] = {
        0x412, 0x441, 0x435, 0x43C, 0x5F, 0x43F, 0x440, 0x438, 
        0x432, 0x435, 0x442 
    }; 
    
    private static String constructString(int [] unicodeChars) {
        StringBuffer buffer = new StringBuffer();
        if (unicodeChars != null) {
            for (int i = 0; i < unicodeChars.length; i++) {
                buffer.append((char)unicodeChars[i]); 
            }
        }
        return buffer.toString();
    }

    public void testMultibyteCodedReadWriteLine() throws Exception {
        String s1 = constructString(SWISS_GERMAN_HELLO);
        String s2 = constructString(RUSSIAN_HELLO);
        String s3 = "Like hello and stuff";
        
        HttpParams params = new DefaultHttpParams(null);
        HttpProtocolParams.setHttpElementCharset(params, "UTF-8");
        
        SessionOutputBuffer outbuf = new SessionOutputBuffer(1024, 16);
        outbuf.reset(params);
        
        for (int i = 0; i < 10; i++) {
            outbuf.writeLine(s1);
            outbuf.writeLine(s2);
            outbuf.writeLine(s3);
        }
        
        ByteArrayOutputStream outstream = new ByteArrayOutputStream(); 
        WritableByteChannel outChannel = newChannel(outstream);
        outbuf.flush(outChannel);

        byte[] tmp = outstream.toByteArray();
        
        ReadableByteChannel channel = newChannel(tmp);        
        SessionInputBuffer inbuf = new SessionInputBuffer(16, 16);
        inbuf.reset(params);
        
        while (inbuf.fill(channel) > 0) {
        }
        
        for (int i = 0; i < 10; i++) {
            assertEquals(s1, inbuf.readLine(true));
            assertEquals(s2, inbuf.readLine(true));
            assertEquals(s3, inbuf.readLine(true));
        }
    }

}