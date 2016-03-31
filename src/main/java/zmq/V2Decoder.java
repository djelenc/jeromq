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

public class V2Decoder extends DecoderBase
{
    private static final int ONE_BYTE_SIZE_READY = 0;
    private static final int EIGHT_BYTE_SIZE_READY = 1;
    private static final int FLAGS_READY = 2;
    private static final int MESSAGE_READY = 3;

    private final byte[] tmpbuf;
    private final ByteBuffer tmpbufWrap;
    private Msg inProgress;
    private final long maxmsgsize;
    private int msgFlags;

    public V2Decoder(int bufsize, long maxmsgsize, ValueReference<Integer> errno)
    {
        super(bufsize, errno);

        this.maxmsgsize = maxmsgsize;

        tmpbuf = new byte[8];
        tmpbufWrap = ByteBuffer.wrap(tmpbuf);
        tmpbufWrap.limit(1);

        //  At the beginning, read one byte and go to ONE_BYTE_SIZE_READY state.
        nextStep(tmpbufWrap, FLAGS_READY);
    }

    @Override
    protected int next()
    {
        switch(state()) {
        case ONE_BYTE_SIZE_READY:
            return oneByteSizeReady();
        case EIGHT_BYTE_SIZE_READY:
            return eightByteSizeReady();
        case FLAGS_READY:
            return flagsReady();
        case MESSAGE_READY:
            return messageReady();
        default:
            throw new AssertionError("Invalid decoder state: " + state());
        }
    }

    private int oneByteSizeReady()
    {
        int size = tmpbuf[0];
        if (size < 0) {
            size = (0xff) & size;
        }

        //  Message size must not exceed the maximum allowed size.
        if (maxmsgsize >= 0) {
            if (size > maxmsgsize) {
                errno.set(ZError.EMSGSIZE);
                return -1;
            }
        }

        // inProgress is initialised at this point
        inProgress = getMsgAllocator().allocate(size);

        inProgress.setFlags(msgFlags);
        nextStep(inProgress, MESSAGE_READY);
        return 0;
    }

    private int eightByteSizeReady()
    {
        //  The payload size is encoded as 64-bit unsigned integer.
        //  The most significant byte comes first.
        tmpbufWrap.position(0);
        tmpbufWrap.limit(8);
        final long msgSize = tmpbufWrap.getLong(0);

        //  Message size must not exceed the maximum allowed size.
        if (maxmsgsize >= 0) {
            if (msgSize > maxmsgsize) {
                errno.set(ZError.EMSGSIZE);
                return -1;
            }
        }

        //  Message size must fit within range of size_t data type.
        if (msgSize > Integer.MAX_VALUE) {
            errno.set(ZError.EMSGSIZE);
            return -1;
        }

        // inProgress is initialised at this point
        inProgress = getMsgAllocator().allocate((int) msgSize);
        inProgress.setFlags(msgFlags);
        nextStep(inProgress, MESSAGE_READY);

        return 0;
    }

    private int flagsReady()
    {
        //  Store the flags from the wire into the message structure.
        msgFlags = 0;
        int first = tmpbuf[0];
        if ((first & V2Protocol.MORE_FLAG) > 0) {
            msgFlags |= Msg.MORE;
        }

        //  The payload length is either one or eight bytes,
        //  depending on whether the 'large' bit is set.
        tmpbufWrap.position(0);
        if ((first & V2Protocol.LARGE_FLAG) > 0) {
            tmpbufWrap.limit(8);
            nextStep(tmpbufWrap, EIGHT_BYTE_SIZE_READY);
        }
        else {
            tmpbufWrap.limit(1);
            nextStep(tmpbufWrap, ONE_BYTE_SIZE_READY);
        }

        return 0;
    }

    private int messageReady()
    {
        // Message is completely read. Signal this to the caller
        // and prepare to decode next message.
        tmpbufWrap.position(0);
        tmpbufWrap.limit(1);
        nextStep(tmpbufWrap, FLAGS_READY);

        return 1;
    }

    @Override
    public Msg msg()
    {
        return inProgress;
    }
}
