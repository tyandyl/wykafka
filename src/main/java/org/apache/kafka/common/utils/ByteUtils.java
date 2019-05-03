/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * This classes exposes low-level methods for reading/writing from byte streams or buffers.
 */
public final class ByteUtils {

    private ByteUtils() {}

    /**
     * Read an unsigned integer from the current position in the buffer, incrementing the position by 4 bytes
     *
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer) {
        return buffer.getInt() & 0xffffffffL;
    }

    /**
     * Read an unsigned integer from the given position without modifying the buffers position
     *
     * @param buffer the buffer to read from
     * @param index the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer, int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }

    /**
     * Read an unsigned integer stored in little-endian format from the {@link InputStream}.
     *
     * @param in The stream to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    public static int readUnsignedIntLE(InputStream in) throws IOException {
        return in.read()
                | (in.read() << 8)
                | (in.read() << 16)
                | (in.read() << 24);
    }

    /**
     * Read an unsigned integer stored in little-endian format from a byte array
     * at a given offset.
     *
     * @param buffer The byte array to read from
     * @param offset The position in buffer to read from
     * @return The integer read (MUST BE TREATED WITH SPECIAL CARE TO AVOID SIGNEDNESS)
     */
    public static int readUnsignedIntLE(byte[] buffer, int offset) {
        return (buffer[offset] << 0 & 0xff)
                | ((buffer[offset + 1] & 0xff) << 8)
                | ((buffer[offset + 2] & 0xff) << 16)
                | ((buffer[offset + 3] & 0xff) << 24);
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param index The position in the buffer at which to begin writing
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, int index, long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param value The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, long value) {
        buffer.putInt((int) (value & 0xffffffffL));
    }

    /**
     * Write an unsigned integer in little-endian format to the {@link OutputStream}.
     *
     * @param out The stream to write to
     * @param value The value to write
     */
    public static void writeUnsignedIntLE(OutputStream out, int value) throws IOException {
        out.write(value);
        out.write(value >>> 8);
        out.write(value >>> 16);
        out.write(value >>> 24);
    }

    /**
     * Write an unsigned integer in little-endian format to a byte array
     * at a given offset.
     *
     * @param buffer The byte array to write to
     * @param offset The position in buffer to write to
     * @param value The value to write
     */
    public static void writeUnsignedIntLE(byte[] buffer, int offset, int value) {
        buffer[offset] = (byte) value;
        buffer[offset + 1] = (byte) (value >>> 8);
        buffer[offset + 2] = (byte) (value >>> 16);
        buffer[offset + 3]   = (byte) (value >>> 24);
    }

    /**
     * Read an integer stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     */
    public static int readVarint(ByteBuffer buffer) {
        int value = 0;
        int i = 0;
        int b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 28)
                throw illegalVarintException(value);
        }
        value |= b << i;
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read an integer stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param in The input to read from
     * @return The integer read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 5 bytes have been read
     * @throws IOException              if {@link DataInput} throws {@link IOException}
     */
    public static int readVarint(DataInput in) throws IOException {
        int value = 0;
        int i = 0;
        int b;
        while (((b = in.readByte()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 28)
                throw illegalVarintException(value);
        }
        value |= b << i;
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read a long stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param in The input to read from
     * @return The long value read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes have been read
     * @throws IOException              if {@link DataInput} throws {@link IOException}
     */
    public static long readVarlong(DataInput in) throws IOException {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = in.readByte()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 63)
                throw illegalVarlongException(value);
        }
        value |= b << i;
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Read a long stored in variable-length format using zig-zag decoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
     *
     * @param buffer The buffer to read from
     * @return The long value read
     *
     * @throws IllegalArgumentException if variable-length value does not terminate after 10 bytes have been read
     */
    public static long readVarlong(ByteBuffer buffer)  {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 63)
                throw illegalVarlongException(value);
        }
        value |= b << i;
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * 原码，反码，补码
     * 正数的反码就是原码，负数的反码等于原码除符号位以外所有的位取反
     * 反码解决了加减运算的问题，但还是有正负零之分，然后就到补码了
     *
     *正数的补码与原码相同，负数的补码为 其原码除符号位外所有位取反（得到反码了），然后最低位加1.
     *
     * http://code.google.com/apis/protocolbuffers/docs/encoding.html
     *
     * https://www.jianshu.com/p/e0d81a9963e9
     *
     * https://www.jianshu.com/p/6f68fb2c7d19
     *
     * https://shift-alt-ctrl.iteye.com/blog/2210885
     *
     * https://blog.csdn.net/zgwangbo/article/details/51590186
     *
     *
     *
     *本文档描述协议缓冲区消息的二进制线格式。在应用程序中使用协议缓冲区不需要理解这一点，
     * 但是了解不同的协议缓冲区格式如何影响编码消息的大小是非常有用的。
     * message Test1 {
     *  optional int32 a = 1;
     *}
     * 在应用程序中，创建一个test1消息并将a设置为150。然后将消息序列化到输出流。如果您能够检查编码的消息，您将看到三个字节：
     * 08 96 01
     * 到目前为止，它很小，而且是数字——但它是什么意思？继续阅读…
     * 要理解简单的协议缓冲区编码，首先需要了解变量。变量是使用一个或多个字节序列化整数的方法。较小的数字占用较小的字节数。
     * 变量中的每个字节（除了最后一个字节）都设置了最高有效位（msb）——这表示还有更多的字节要到来。
     * 每个字节的下7位用于将数字的两个补码表示存储在7位的组中，最低有效组优先。
     * 例如，这里是数字1——它是一个单字节，所以没有设置msb：
     * 0000 0001
     * 这里是300——这有点复杂：
     * 1010 1100 0000 0010
     *你怎么知道这是300？首先，从每个字节中删除msb，因为这只是为了告诉我们是否已到达数字的结尾
     * （如您所见，它在第一个字节中设置，因为变量中有多个字节）：
     * →1010 1100 0000 0010
     * → 010 1100  000 0010
     *您将这两组7位颠倒过来，因为正如您所记得的，变量首先存储具有最低有效组的数字。然后连接它们以获得最终值：
     * 先获取上边右侧
     * →  000 0010  010 1100
     * →  000 0010 ++ 010 1100
     * →  100101100
     * →  256 + 32 + 8 + 4 = 300
     *
     *
     * 如您所知，协议缓冲区消息是一系列键值对。
     * 消息的二进制版本只使用字段的编号作为键–每个字段的名称和声明的类型只能在解码端通过引用消息类型的定义（即.proto文件）来确定。
     * 当对消息进行编码时，键和值将连接到字节流中。当消息被解码时，解析器需要能够跳过它不能识别的字段。这样，新字段就可以添加到消息中，
     * 而不会破坏不了解它们的旧程序。为此，Wire格式消息中每对的“键”实际上是两个值——来自.proto文件的字段号，
     * 加上提供足够信息以查找以下值的线类型。在大多数语言实现中，这个键被称为标记。
     *
     * Type	Meaning	            Used For
     *  0	Varint	            int32, int64, uint32, uint64, sint32, sint64, bool, enum
     *  1	64-bit	            fixed64, sfixed64, double
     *  2	Length-delimited	string, bytes, embedded messages, packed repeated fields
     *  3	Start group	        groups (deprecated)
     *  4	End group	        groups (deprecated)
     *  5	32-bit	            fixed32, sfixed32, float

     */
    public static void writeVarint(int value, DataOutput out) throws IOException {
        //左移运算符<<使指定值的所有位都左移规定的次数。
        //1）它的通用格式如下所示：
        // value << num
        // num 指定要移位值value 移动的位数。
        // 左移的规则只记住一点：丢弃最高位，0补最低位
        // 如果移动的位数超过了该类型的最大位数，那么编译器会对移动的位数取模。如对int型移动33位，实际上只移动了33%32=1位。
        //2）运算规则
        // 按二进制形式把所有的数字向左移动对应的位数，高位移出(舍弃)，低位的空位补零。
        // 当左移的运算数是int 类型时，每移动1位它的第31位就要被移出并且丢弃；
        // 当左移的运算数是long 类型时，每移动1位它的第63位就要被移出并且丢弃。
        // 当左移的运算数是byte 和short类型时，将自动把这些类型扩大为 int 型。

        int v = (value << 1) ^ (value >> 31);
        while ((v & 0xffffff80) != 0L) {
            out.writeByte((v & 0x7f) | 0x80);
            v >>>= 7;
        }
        out.writeByte((byte) v);
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The output to write to
     */
    public static void writeVarint(int value, ByteBuffer buffer) {
        int v = (value << 1) ^ (value >> 31);
        while ((v & 0xffffff80) != 0L) {
            byte b = (byte) ((v & 0x7f) | 0x80);
            buffer.put(b);
            v >>>= 7;
        }
        buffer.put((byte) v);
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the output.
     *
     * @param value The value to write
     * @param out The output to write to
     */
    public static void writeVarlong(long value, DataOutput out) throws IOException {
        long v = (value << 1) ^ (value >> 63);
        while ((v & 0xffffffffffffff80L) != 0L) {
            out.writeByte(((int) v & 0x7f) | 0x80);
            v >>>= 7;
        }
        out.writeByte((byte) v);
    }

    /**
     * Write the given integer following the variable-length zig-zag encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>
     * into the buffer.
     *
     * @param value The value to write
     * @param buffer The buffer to write to
     */
    public static void writeVarlong(long value, ByteBuffer buffer) {
        long v = (value << 1) ^ (value >> 63);
        while ((v & 0xffffffffffffff80L) != 0L) {
            byte b = (byte) ((v & 0x7f) | 0x80);
            buffer.put(b);
            v >>>= 7;
        }
        buffer.put((byte) v);
    }

    /**
     * Number of bytes needed to encode an integer in variable-length format.
     *
     * @param value The signed value
     */
    public static int sizeOfVarint(int value) {
        int v = (value << 1) ^ (value >> 31);
        int bytes = 1;
        while ((v & 0xffffff80) != 0L) {
            bytes += 1;
            v >>>= 7;
        }
        return bytes;
    }

    /**
     * Number of bytes needed to encode a long in variable-length format.
     *
     * @param value The signed value
     */
    public static int sizeOfVarlong(long value) {
        long v = (value << 1) ^ (value >> 63);
        int bytes = 1;
        while ((v & 0xffffffffffffff80L) != 0L) {
            bytes += 1;
            v >>>= 7;
        }
        return bytes;
    }

    private static IllegalArgumentException illegalVarintException(int value) {
        throw new IllegalArgumentException("Varint is too long, the most significant bit in the 5th byte is set, " +
                "converted value: " + Integer.toHexString(value));
    }

    private static IllegalArgumentException illegalVarlongException(long value) {
        throw new IllegalArgumentException("Varlong is too long, most significant bit in the 10th byte is set, " +
                "converted value: " + Long.toHexString(value));
    }
}
