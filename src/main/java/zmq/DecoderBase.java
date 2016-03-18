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

//  Helper base class for decoders that know the amount of data to read
//  in advance at any moment. Knowing the amount in advance is a property
//  of the protocol used. 0MQ framing protocol is based size-prefixed
//  paradigm, which qualifies it to be parsed by this class.
//  On the other hand, XML-based transports (like XMPP or SOAP) don't allow
//  for knowing the size of data to read in advance and should use different
//  decoding algorithms.
//
//  This class implements the state machine that parses the incoming buffer.
//  Derived class should implement individual state machine actions.

public abstract class DecoderBase implements IDecoder
{
    //  Where to store the read data.
    private ByteBuffer readBuf;
    private MsgAllocator msgAllocator = new MsgAllocatorHeap();

    //  The buffer for data to decode.
    private int bufsize;
    private ByteBuffer buf;

    private int state;

    boolean zeroCopy;

    public DecoderBase(int bufsize)
    {
        state = -1;
        this.bufsize = bufsize;
        if (bufsize > 0) {
            buf = ByteBuffer.allocateDirect(bufsize);
        }
        readBuf = null;
        zeroCopy = false;
    }

    //  Returns a buffer to be filled with binary data.
    public ByteBuffer getBuffer()
    {
        //  If we are expected to read large message, we'll opt for zero-
        //  copy, i.e. we'll ask caller to fill the data directly to the
        //  message. Note that subsequent read(s) are non-blocking, thus
        //  each single read reads at most SO_RCVBUF bytes at once not
        //  depending on how large is the chunk returned from here.
        //  As a consequence, large messages being received won't block
        //  other engines running in the same I/O thread for excessive
        //  amounts of time.

        if (readBuf.remaining() >= bufsize) {
            zeroCopy = true;
            return readBuf.duplicate();
        }
        else {
            zeroCopy = false;
            buf.clear();
            return buf;
        }
    }


    /** TODO: This method is not correctly implemented yet!
     * Processes the data in the buffer previously allocated using {@link #getBuffer}.
     *
     * @param buf  Buffer to decode
     * @param size number of bytes actually filled into the buffer
     * @return 1 when the whole message decodes; 0 when more data is needed; -1 when an error occurs
     */
    @Override
    public int decode(ByteBuffer buf, int size)
    {
        //  In case of zero-copy simply adjust the pointers, no copying
        //  is required. Also, run the state machine in case all the data
        //  were processed.
        if (zeroCopy) {
            readBuf.position(readBuf.position() + size);

            while (readBuf.remaining() == 0) {
                final int rc = next();

                if (rc != 0) {
                    return rc;
                }
            }
            return 0;
        }

        int pos = 0;
        while (true) {
            //  Try to get more space in the message to fill in.
            //  If none is available, return.
            while (readBuf.remaining() == 0) {
                final int rc = next();
                if (rc != 0) {
                    if (state() < 0) {
                        return -1;
                    }

                    return pos;
                }
            }

            //  If there are no more data in the buffer, return.
            if (pos == size) {
                return pos;
            }

            //  Copy the data from buffer to the message.
            int toCopy = Math.min(readBuf.remaining(), size - pos);
            int limit = buf.limit();
            buf.limit(buf.position() + toCopy);
            readBuf.put(buf);
            buf.limit(limit);
            pos += toCopy;
        }
    }

    protected void nextStep(Msg msg, int state)
    {
        nextStep(msg.buf(), state);
    }

    protected void nextStep(byte[] buf, int toRead, int state)
    {
        readBuf = ByteBuffer.wrap(buf);
        readBuf.limit(toRead);
        this.state = state;
    }

    protected void nextStep(ByteBuffer buf, int state)
    {
        readBuf = buf;
        this.state = state;
    }

    protected int state()
    {
        return state;
    }

    protected void state(int state)
    {
        this.state = state;
    }

    /*protected void decodingError()
    {
        state(-1);
    }*/

    public MsgAllocator getMsgAllocator()
    {
       return msgAllocator;
    }

    public void setMsgAllocator(MsgAllocator msgAllocator)
    {
       this.msgAllocator = msgAllocator;
    }

    protected abstract int next();
}
