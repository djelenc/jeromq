/*
    Copyright (c) 2007-2014 Contributors as noted in the AUTHORS file

    This file is part of 0MQ.

    0MQ is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    0MQ is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package zmq;

import java.nio.ByteBuffer;

// Encoder for 0MQ framing protocol. Converts messages into data stream.

public class V2Encoder extends EncoderBase
{
    private static final int SIZE_READY = 0;
    private static final int MESSAGE_READY = 1;

    private final byte[] tmpbuf;
    private final ByteBuffer tmpbufWrap;

    public V2Encoder(int bufsize)
    {
        super(bufsize);
        tmpbuf = new byte[9];
        tmpbufWrap = ByteBuffer.wrap(tmpbuf);

        //  Write 0 bytes to the batch and go to messageReady state.
        nextStep(null, 0, MESSAGE_READY, true);
    }

    @Override
    protected void next()
    {
        switch(state()) {
        case SIZE_READY:
            sizeReady();
        case MESSAGE_READY:
            messageReady();
        default:
            assert false : "Invalid encoder state: " + state();
        }
    }

    private void sizeReady()
    {
        //  Write message body into the buffer.
        nextStep(inProgress.buf(), MESSAGE_READY, true);
    }

    private void messageReady()
    {
        int protocolFlags = 0;
        if (inProgress.hasMore()) {
            protocolFlags |= V2Protocol.MORE_FLAG;
        }
        if (inProgress.size() > 255) {
            protocolFlags |= V2Protocol.LARGE_FLAG;
        }
        tmpbuf[0] = (byte) protocolFlags;

        //  Encode the message length. For messages less then 256 bytes,
        //  the length is encoded as 8-bit unsigned integer. For larger
        //  messages, 64-bit unsigned integer in network byte order is used.
        final int size = inProgress.size();
        tmpbufWrap.position(0);
        if (size > 255) {
            tmpbufWrap.limit(9);
            tmpbufWrap.putLong(1, size);
            nextStep(tmpbufWrap, SIZE_READY, false);
        }
        else {
            tmpbufWrap.limit(2);
            tmpbuf[1] = (byte) (size);
            nextStep(tmpbufWrap, SIZE_READY, false);
        }
    }
}