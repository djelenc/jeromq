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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

public class StreamEngine implements IEngine, IPollEvents
{
    enum Protocol
    {
        ZMTP_1_0((byte) 0),
        ZMTP_2_0((byte) 1);

        public final byte value;

        Protocol(byte b)
        {
            value = b;
        }
    }

    //  Size of the greeting message:
    //  Preamble (10 bytes) + version (1 byte) + socket type (1 byte).
    private static final int GREETING_SIZE = 12;

    private SocketChannel handle;

    private ByteBuffer inbuf;
    private int insize;
    private DecoderBase decoder;

    private Transfer outbuf;
    private int outsize;
    private EncoderBase encoder;

    private boolean ioError;

    /**
     * True iff the session could not accept more messages due to flow control.
     */
    private boolean congested;

    /**
     * True iff the engine has received identity message.
     */
    private boolean identityReceived;

    /**
     * True iff the engine has sent identity message.
     */
    private boolean identitySent;

    /**
     * True iff the engine has received all ZMTP control messages.
     */
    private boolean rxInitialized;

    /**
     * True iff the engine has sent all ZMTP control messages.
     */
    private boolean txInitialized;

    /**
     * Indicates whether the engine is to inject a phony subscription message
     * into the incoming stream. Needed to support old peers.
     */
    private boolean subscriptionRequired;

    private Msg txMsg;

    //  When true, we are still trying to determine whether
    //  the peer is using versioned protocol, and if so, which
    //  version.  When false, normal message flow has started.
    private boolean handshaking;

    //  The receive buffer holding the greeting message
    //  that we are receiving from the peer.
    private final ByteBuffer greeting;

    //  The send buffer holding the greeting message
    //  that we are sending to the peer.
    private final ByteBuffer greetingOutputBuffer;

    //  The session this engine is attached to.
    private SessionBase session;

    private Options options;

    // String representation of endpoint
    private String endpoint;

    private boolean plugged;

    // Socket
    private SocketBase socket;

    private IOObject ioObject;

    public StreamEngine(SocketChannel handle, final Options options, final String endpoint)
    {
        this.handle = handle;
        inbuf = null;
        insize = 0;
        ioError = false;
        congested = false;
        identityReceived = false;
        identitySent = false;
        rxInitialized = false;
        txInitialized = false;
        subscriptionRequired = false;
        txMsg = new Msg();
        outbuf = null;
        outsize = 0;
        handshaking = true;
        session = null;
        this.options = options;
        plugged = false;
        this.endpoint = endpoint;
        socket = null;
        greeting = ByteBuffer.allocate(GREETING_SIZE).order(ByteOrder.BIG_ENDIAN);
        greetingOutputBuffer = ByteBuffer.allocate(GREETING_SIZE).order(ByteOrder.BIG_ENDIAN);
        encoder = null;
        decoder = null;

        //  Put the socket into non-blocking mode.
        try {
            Utils.unblockSocket(this.handle);

            //  Set the socket buffer limits for the underlying socket.
            if (this.options.sndbuf != 0) {
                this.handle.socket().setSendBufferSize(this.options.sndbuf);
            }
            if (this.options.rcvbuf != 0) {
                this.handle.socket().setReceiveBufferSize(this.options.rcvbuf);
            }
        }
        catch (IOException e) {
            throw new ZError.IOException(e);
        }
    }

    private DecoderBase newDecoder(int size, long max, SessionBase session, Protocol version)
    {
       DecoderBase decoder;
       if (options.decoder == null) {
            if (version == Protocol.ZMTP_2_0) {
               decoder = new V2Decoder(size, max, session.getSocket().errno);
            }
            else {
               decoder = new V1Decoder(size, max, session.getSocket().errno);
            }
        }
        else {
           try {
               Constructor<? extends DecoderBase> dcon;

               if (version == Protocol.ZMTP_1_0)  {
                   dcon = options.decoder.getConstructor(int.class, long.class, ValueReference.class);
                   decoder = dcon.newInstance(size, max, session.getSocket().errno);
               }
               else {
                   // fixme??
                   dcon = options.decoder.getConstructor(int.class, long.class, ValueReference.class);
                   decoder = dcon.newInstance(size, max, session.getSocket().errno);
               }
           }
           catch (SecurityException e) {
               throw new ZError.InstantiationException(e);
           }
           catch (NoSuchMethodException e) {
               throw new ZError.InstantiationException(e);
           }
           catch (InvocationTargetException e) {
               throw new ZError.InstantiationException(e);
           }
           catch (IllegalAccessException e) {
               throw new ZError.InstantiationException(e);
           }
           catch (InstantiationException e) {
               throw new ZError.InstantiationException(e);
           }
        }

        if (options.msgAllocator != null) {
           decoder.setMsgAllocator(options.msgAllocator);
        }
        return decoder;
    }

    private EncoderBase newEncoder(int size, Protocol version)
    {
        if (options.encoder == null) {
            if (version == Protocol.ZMTP_2_0) {
                return new V2Encoder(size);
            }
            return new V1Encoder(size);
        }

        try {
            Constructor<? extends EncoderBase> econ;

            if (version == Protocol.ZMTP_1_0) {
                econ = options.encoder.getConstructor(int.class);
                return econ.newInstance(size);
            }
            else {
                // fixme??
                econ = options.encoder.getConstructor(int.class);
                return econ.newInstance(size);
            }
        }
        catch (SecurityException e) {
            throw new ZError.InstantiationException(e);
        }
        catch (NoSuchMethodException e) {
            throw new ZError.InstantiationException(e);
        }
        catch (InvocationTargetException e) {
            throw new ZError.InstantiationException(e);
        }
        catch (IllegalAccessException e) {
            throw new ZError.InstantiationException(e);
        }
        catch (InstantiationException e) {
            throw new ZError.InstantiationException(e);
        }
    }

    public void destroy()
    {
        assert (!plugged);

        if (handle != null) {
            try {
                handle.close();
            }
            catch (IOException e) {
            }
            handle = null;
        }
    }

    public void plug(IOThread ioThread, SessionBase session)
    {
        assert (!plugged);
        plugged = true;

        //  Connect to session object.
        assert (this.session == null);
        assert (session != null);
        this.session = session;
        socket = this.session.getSocket();

        ioObject = new IOObject(null);
        ioObject.setHandler(this);
        //  Connect to I/O threads poller object.
        ioObject.plug(ioThread);
        ioObject.addHandle(handle);
        ioError = false;

        //  Send the 'length' and 'flags' fields of the identity message.
        //  The 'length' field is encoded in the long format.
        greetingOutputBuffer.put((byte) 0xff);
        greetingOutputBuffer.putLong(options.identitySize + 1);
        greetingOutputBuffer.put((byte) 0x7f);

        ioObject.setPollIn(handle);
        //  When there's a raw custom encoder, we don't send 10 bytes frame
        boolean custom;
        try {
            custom = options.encoder != null && options.encoder.getDeclaredField("RAW_ENCODER") != null;
        }
        catch (NoSuchFieldException e) {
            custom = false;
        }

        if (!custom) {
            outsize = greetingOutputBuffer.position();
            greetingOutputBuffer.flip();
            outbuf = new Transfer.ByteBufferTransfer(greetingOutputBuffer);
            ioObject.setPollOut(handle);
        }

        //  Flush all the data that may have been already received downstream.
        inEvent();
    }

    private void unplug()
    {
        assert (plugged);
        plugged = false;

        //  Cancel all fd subscriptions.
        if (!ioError) {
            ioObject.removeHandle(handle);
        }

        //  Disconnect from I/O threads poller object.
        ioObject.unplug();
        session = null;
    }

    @Override
    public void terminate()
    {
        unplug();
        destroy();
    }

    @Override
    public void inEvent()
    {
        assert !ioError;

        //  If still handshaking, receive and process the greeting message.
        if (handshaking) {
            if (!handshake()) {
                return;
            }
        }

        assert (decoder != null);

        //  If there has been an I/O error, stop polling.
        if (congested) {
            ioObject.removeHandle(handle);
            ioError = true;
            return;
        }

        //  If there's no data to process in the buffer...
        if (insize == 0) {
            //  Retrieve the buffer and read as much data as possible.
            //  Note that buffer can be arbitrarily large. However, we assume
            //  the underlying TCP layer has fixed buffer size and thus the
            //  number of bytes read will be always limited.
            inbuf = decoder.getBuffer();
            insize = read(inbuf);
            inbuf.flip();

            //  Check whether the peer has closed the connection.
            if (insize == -1) {
                error();
                return;
            }
        }


        int rc = 0;
        final IntReference processed = new IntReference(0);

        while (insize > 0) {
            // fixme: remove processed and work with with inbuf pos() and limit()
            rc = decoder.decode(inbuf, insize, processed);
            assert (processed.get() <= insize);
            insize -= processed.get();
            if (rc == 0 || rc == -1)
                break;
            rc = writeMsg(decoder.msg());
            if (rc == -1 || rc == ZError.EAGAIN)
                break;
        }

        // Tear down the connection if we have failed to decode input data
        // or the session has rejected the message.
        if (rc == -1) {
            error();
            return;
        } else
        if (rc == ZError.EAGAIN) {
            congested = true;
            ioObject.resetPollIn(handle);
        }

        session.flush();
    }

    @Override
    public void outEvent()
    {
        assert !ioError;

        //  If write buffer is empty, try to read new data from the encoder.
        if (outsize == 0) {
            //  Even when we stop polling as soon as there is no
            //  data to send, the poller may invoke outEvent one
            //  more time due to 'speculative write' optimisation.
            if (encoder == null) {
                 assert (handshaking);
                 return;
            }

            //outbuf = encoder.getData(null);
            outbuf = encoder.encode(null);
            outsize = outbuf.remaining();

            while (outsize < Config.OUT_BATCH_SIZE.getValue()) {
                txMsg = readMsg();

                if (txMsg == null)
                    break;
                encoder.loadMsg(txMsg);
                
                unsigned char *bufptr = outpos + outsize;
                size_t n = encoder->encode (&bufptr, out_batch_size - outsize);
                zmq_assert (n > 0);
                if (outpos == NULL)
                    outpos = bufptr;
                outsize += n;
            }


            //  If there is no data to send, stop polling for output.
            if (outbuf.remaining() == 0) {
                ioObject.resetPollOut(handle);

                // when we use custom encoder, we might want to close
                if (encoder.isError()) {
                    error();
                }

                return;
            }
        }

        //  If there are any data to write in write buffer, write as much as
        //  possible to the socket. Note that amount of data to write can be
        //  arbitratily large. However, we assume that underlying TCP layer has
        //  limited transmission buffer and thus the actual number of bytes
        //  written should be reasonably modest.
        int nbytes = write(outbuf);

        //  IO error has occurred. We stop waiting for output events.
        //  The engine is not terminated until we detect input error;
        //  this is necessary to prevent losing incomming messages.
        if (nbytes == -1) {
            ioObject.resetPollOut(handle);
            return;
        }

        outsize -= nbytes;

        //  If we are still handshaking and there are no data
        //  to send, stop polling for output.
        if (handshaking) {
            if (outsize == 0) {
                ioObject.resetPollOut(handle);
            }
        }

        // when we use custom encoder, we might want to close after sending a response
        if (outsize == 0) {
            if (encoder != null && encoder.isError()) {
                error();
            }
        }
    }

    @Override
    public void connectEvent()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void acceptEvent()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void timerEvent(int id)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void activateOut()
    {
        if (ioError)
            return;

        ioObject.setPollOut(handle);

        //  Speculative write: The assumption is that at the moment new message
        //  was sent by the user the socket is probably available for writing.
        //  Thus we try to write the data to socket avoiding polling for POLLOUT.
        //  Consequently, the latency should be better in request/reply scenarios.
        outEvent();
    }

    @Override
    public void activateIn()
    {
        if (!ioEnabled) {
            //  There was an input error but the engine could not
            //  be terminated (due to the stalled decoder).
            //  Flush the pending message and terminate the engine now.
            decoder.processBuffer(inbuf, 0);
            assert (!decoder.stalled());
            session.flush();
            error();
            return;
        }

        ioObject.setPollIn(handle);

        //  Speculative read.
        ioObject.inEvent();
    }

    private boolean handshake()
    {
        assert (handshaking);

        //  Receive the greeting.
        while (greeting.position() < GREETING_SIZE) {
            final int n = read(greeting);
            if (n == -1) {
                error();
                return false;
            }

            if (n == 0) {
                return false;
            }

            //  We have received at least one byte from the peer.
            //  If the first byte is not 0xff, we know that the
            //  peer is using unversioned protocol.
            if ((greeting.get(0) & 0xff) != 0xff) {
                break;
            }

            if (greeting.position() < 10) {
                continue;
            }

            //  Inspect the right-most bit of the 10th byte (which coincides
            //  with the 'flags' field if a regular message was sent).
            //  Zero indicates this is a header of identity message
            //  (i.e. the peer is using the unversioned protocol).
            if ((greeting.get(9) & 0x01) == 0) {
                break;
            }

            //  The peer is using versioned protocol.
            //  Send the rest of the greeting, if necessary.
            if (greetingOutputBuffer.limit() < GREETING_SIZE) {
                if (outsize == 0) {
                    ioObject.setPollOut(handle);
                }
                int pos = greetingOutputBuffer.position();
                greetingOutputBuffer.position(10).limit(GREETING_SIZE);
                greetingOutputBuffer.put((byte) 1); // Protocol version
                greetingOutputBuffer.put((byte) options.type);  // Socket type
                greetingOutputBuffer.position(pos);
                outsize += 2;
            }
        }

        //  Position of the version field in the greeting.
        final int versionPos = 10;

        //  Is the peer using ZMTP/1.0 with no revision number?
        //  If so, we send and receive rest of identity message
        if ((greeting.get(0) & 0xff) != 0xff || (greeting.get(9) & 0x01) == 0) {
            encoder = newEncoder(Config.OUT_BATCH_SIZE.getValue(), Protocol.ZMTP_1_0);

            decoder = newDecoder(Config.IN_BATCH_SIZE.getValue(), options.maxMsgSize, null, Protocol.ZMTP_1_0);

            //  We have already sent the message header.
            //  Since there is no way to tell the encoder to
            //  skip the message header, we simply throw that
            //  header data away.
            final int headerSize = options.identitySize + 1 >= 255 ? 10 : 2;
            ByteBuffer tmp = ByteBuffer.allocate(headerSize);
            encoder.encode(tmp); // fixme?
            if (tmp.remaining() != headerSize) {
                return false;
            }

            //  Make sure the decoder sees the data we have already received.
            inbuf = greeting;
            greeting.flip();
            insize = greeting.remaining();

            //  To allow for interoperability with peers that do not forward
            //  their subscriptions, we inject a phony subscription
            //  message into the incoming message stream.
            if (options.type == ZMQ.ZMQ_PUB || options.type == ZMQ.ZMQ_XPUB) {
                subscriptionRequired = true;
            }
        }
        else
        if (greeting.get(versionPos) == Protocol.ZMTP_1_0.value) {
            //  ZMTP/1.0 framing.
            encoder = newEncoder(Config.OUT_BATCH_SIZE.getValue(), Protocol.ZMTP_1_0);
            decoder = newDecoder(Config.IN_BATCH_SIZE.getValue(), options.maxMsgSize, null, Protocol.ZMTP_1_0);
        }
        else {
            //  ZMTP/2.0 framing protocol.
            encoder = newEncoder(Config.OUT_BATCH_SIZE.getValue(), Protocol.ZMTP_2_0);
            decoder = newDecoder(Config.IN_BATCH_SIZE.getValue(), options.maxMsgSize, session, Protocol.ZMTP_2_0);
        }
        // Start polling for output if necessary.
        if (outsize == 0) {
            ioObject.setPollOut(handle);
        }

        //  Handshaking was successful.
        //  Switch into the normal message flow.
        handshaking = false;

        return true;
    }

    private void error()
    {
        assert (session != null);
        socket.eventDisconnected(endpoint, handle);
        session.flush();
        session.detach();
        unplug();
        destroy();
    }

    /**
     * Writes data to the socket.
     *
     * @param buf Buffer to write
     * @return Number of bytes actually written (even zero is considered a
     * success). In case of error or orderly shutdown by the other peer, -1
     * is returned.
     */
    private int write(Transfer buf)
    {
        try {
            return buf.transferTo(handle);
        }
        catch (IOException e) {
            return -1;
        }
    }

    /**
     * Reads data from the socket and stores it into buf.
     *
     * @param buf Buffer to hold the read data
     * @return Number of bytes actually read (even zero is considered a
     * success). In case of error or orderly shutdown by the other peer,
     * -1 is returned.
     */
    private int read(ByteBuffer buf)
    {
        try {
            return handle.read(buf);
        }
        catch (IOException e) {
            return -1;
        }
    }

    /**
     * Fetches a message from the session (to be written to the socket)
     * @return A message, null if none available
     */
    private Msg readMsg()
    {
        boolean custom;
        try {
            custom = options.encoder != null && options.encoder.getDeclaredField("RAW_ENCODER") != null;
        } catch (NoSuchFieldException e) {
            custom = false;
        }

        if (txInitialized || custom)
            return session.pullMsg();

        if (!identitySent) {
            final Msg msg = new Msg(options.identitySize);
            msg.put(options.identity, 0, options.identitySize);
            identitySent = true;
            txInitialized = true;
            return msg;
        }

        txInitialized = true;
        return null;
    }

    /**
     * Pushes the message (previously read from a socket) to the session
     * @param msg message
     * @return 0 on success, -1 on error, {@link ZError#EAGAIN} when a retry is required
     */
    private int writeMsg(Msg msg)
    {
        boolean custom;
        try {
            custom = options.encoder != null && options.encoder.getDeclaredField("RAW_ENCODER") != null;
        } catch (NoSuchFieldException e) {
            custom = false;
        }

        if (rxInitialized || custom)
            return session.pushMsg(msg);

        if (!identityReceived) {
            if (options.recvIdentity) {
                msg.setFlags(Msg.IDENTITY);
                final int rc = session.pushMsg(msg);
                if (rc == -1)
                    return -1;
            }

            identityReceived = true;
        }

        // Inject the subscription message, so that also
        // ZMQ 2.x peers receive published messages.
        if (subscriptionRequired) {
            final Msg subscriptionMsg = new Msg(1);
            subscriptionMsg.put((byte) 1);
            final int rc = session.pushMsg(subscriptionMsg);

            if (rc == -1)
                return -1;

            subscriptionRequired = false;
        }

        rxInitialized = true;
        return 0;
    }
}
