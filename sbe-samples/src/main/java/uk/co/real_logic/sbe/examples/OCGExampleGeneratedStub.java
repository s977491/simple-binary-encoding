/*
 * Copyright 2013-2018 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.sbe.examples;

import baseline.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static java.nio.file.StandardOpenOption.*;

public class OCGExampleGeneratedStub
{
    private static final String ENCODING_FILENAME = "sbe.encoding.filename";

    private static final DummyMsgHeaderDecoder MESSAGE_HEADER_DECODER = new DummyMsgHeaderDecoder();
    private static final DummyMsgHeaderEncoder MESSAGE_HEADER_ENCODER = new DummyMsgHeaderEncoder();
    private static final OcgDecoder OCG_DECODER = new OcgDecoder();
    private static final OcgEncoder OCG_ENCODER = new OcgEncoder();

    static
    {
    }

    public static void main(final String[] args) throws Exception
    {
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(MESSAGE_HEADER_DECODER.blockLength());
        final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);


        File file = new File(OCGExampleGeneratedStub.class.getClassLoader().getResource("ocg/CO99999902_bin_send_0001.log").getFile());

        try (FileChannel channel = FileChannel.open(file.toPath(), READ))
        {
            byteBuffer.limit(OCG_DECODER.sbeBlockLength());
            int ret;
            do {
                ret = channel.read(byteBuffer);
                if (ret == -1) {
                    throw new Exception("file end prematurely");
                }
            } while (byteBuffer.limit() != byteBuffer.position());

            int bufferOffset = 0;
            OCG_DECODER.wrap(directBuffer, bufferOffset, OCG_DECODER.sbeBlockLength(), MESSAGE_HEADER_DECODER.version());

            System.out.println(OCG_DECODER.toString());
        }
    }
}
