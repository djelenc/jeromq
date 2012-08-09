package zmq;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Router extends SocketBase {

    public static class RouterSession extends SessionBase {
        public RouterSession(IOThread io_thread_, boolean connect_,
            SocketBase socket_, final Options options_,
            final Address addr_) {
            super(io_thread_, connect_, socket_, options_, addr_);
        }
    }
    
    //  Fair queueing object for inbound pipes.
    private final FQ fq;

    //  True iff there is a message held in the pre-fetch buffer.
    private boolean prefetched;

    //  If true, the receiver got the message part with
    //  the peer's identity.
    private boolean identity_sent;

    //  Holds the prefetched identity.
    private Msg prefetched_id;

    //  Holds the prefetched message.
    private Msg prefetched_msg;

    //  If true, more incoming message parts are expected.
    private boolean more_in;

    class Outpipe
    {
        private Pipe pipe;
        private boolean active;
        
        public Outpipe(Pipe pipe_, boolean active_) {
            pipe = pipe_;
            active = active_;
        }
    };

    //  We keep a set of pipes that have not been identified yet.
    private final Set <Pipe> anonymous_pipes;

    //  Outbound pipes indexed by the peer IDs.
    private final Map <Blob, Outpipe> outpipes;

    //  The pipe we are currently writing to.
    private Pipe current_out;

    //  If true, more outgoing message parts are expected.
    private boolean more_out;

    //  Peer ID are generated. It's a simple increment and wrap-over
    //  algorithm. This value is the next ID to use (if not used already).
    private int next_peer_id;

    // If true, report EAGAIN to the caller instead of silently dropping 
    // the message targeting an unknown peer.
    private boolean report_unroutable;

    
    public Router(Ctx parent_, int tid_, int sid_) {
        super(parent_, tid_, sid_);
        prefetched = false;
        identity_sent = false;
        more_in = false;
        current_out = null;
        more_out = false;
        next_peer_id = Utils.generate_random (); 
        report_unroutable = false;
        
        options.type = ZMQ.ZMQ_ROUTER;
        
        
        fq = new FQ();
        prefetched_id = new Msg();
        prefetched_msg = new Msg();
        
        anonymous_pipes = new HashSet<Pipe>();
        outpipes = new HashMap<Blob, Outpipe>();
        
        //  TODO: Uncomment the following line when ROUTER will become true ROUTER
        //  rather than generic router socket.
        //  If peer disconnect there's noone to send reply to anyway. We can drop
        //  all the outstanding requests from that peer.
        //  options.delay_on_disconnect = false;
            
        options.send_identity = true;
        options.recv_identity = true;
            
    }


    
    @Override
    public void xattach_pipe(Pipe pipe_, boolean icanhasall_) {
        assert (pipe_ != null);

        boolean identity_ok = identify_peer (pipe_);
        if (identity_ok)
            fq.attach (pipe_);
        else
            anonymous_pipes.add (pipe_);
    }
    
    @Override
    public void xsetsockopt (int option_, int optval_)
    {
        if (option_ != ZMQ.ZMQ_ROUTER_BEHAVIOR) {
            return;
        }
        report_unroutable = optval_ == 1;
    }


    @Override
    public void xterminated(Pipe pipe_) {
        if (!anonymous_pipes.remove(pipe_)) {
            
            assert(outpipes.remove(pipe_.get_identity()) != null);
            fq.terminated (pipe_);
            if (pipe_ == current_out)
                current_out = null;
        }
    }
    
    @Override
    public void xread_activated (Pipe pipe_)
    {
        if (!anonymous_pipes.contains(pipe_)) 
            fq.activated (pipe_);
        else {
            boolean identity_ok = identify_peer (pipe_);
            if (identity_ok) {
                anonymous_pipes.remove(pipe_);
                fq.attach (pipe_);
            }
        }
    }
    
    @Override
    public void xwrite_activated (Pipe pipe_)
    {
        for (Map.Entry<Blob, Outpipe> it: outpipes.entrySet()) {
            if (it.getValue().pipe == pipe_) {
                assert (!it.getValue().active);
                it.getValue().active = true;
                return;
            }
        }
        assert (false);
    }
    
    @Override
    protected boolean xsend (Msg msg_, int flags_)
    {
        //  If this is the first part of the message it's the ID of the
        //  peer to send the message to.
        if (!more_out) {
            assert (current_out == null);

            //  If we have malformed message (prefix with no subsequent message)
            //  then just silently ignore it.
            //  TODO: The connections should be killed instead.
            if (msg_.has_more()) {

                more_out = true;

                //  Find the pipe associated with the identity stored in the prefix.
                //  If there's no such pipe just silently ignore the message, unless
                //  report_unreachable is set.
                Blob identity = new Blob(msg_.data());
                Outpipe op = outpipes.get(identity);

                if (op != null) {
                    current_out = op.pipe;
                    if (!current_out.check_write ()) {
                        op.active = false;
                        current_out = null;
                    }
                } else if (report_unroutable) {
                    more_out = false;
                    return false;
                }
            }

            return true;
        }

        //  Check whether this is the last part of the message.
        more_out = msg_.has_more();

        //  Push the message into the pipe. If there's no out pipe, just drop it.
        if (current_out != null) {
            boolean ok = current_out.write (msg_);
            if (!ok)
                current_out = null;
            else if (!more_out) {
                current_out.flush ();
                current_out = null;
            }
        }
        else {
        }

        //  Detach the message from the data buffer.

        return true;
    }


    @Override
    protected Msg xrecv (int flags_)
    {
        Msg msg_ = null;
        if (prefetched) {
            if (!identity_sent) {
                msg_ = prefetched_id;
                identity_sent = true;
            }
            else {
                msg_ = prefetched_msg;
                prefetched = false;
            }
            more_in = msg_.has_more();
            return msg_;
        }

        Pipe[] pipe = new Pipe[1];
        msg_ = fq.recvpipe (pipe);
        if (pipe[0] == null) {
            return null;
        }

        //  Identity is not expected
        assert ((msg_.flags () & Msg.identity) == 0);
        assert (pipe != null);

        //  If we are in the middle of reading a message, just return the next part.
        if (more_in)
            more_in = msg_.has_more();
        else {
            //  We are at the beginning of a message.
            //  Keep the message part we have in the prefetch buffer
            //  and return the ID of the peer instead.
            prefetched_msg = msg_;

            prefetched = true;

            Blob identity = pipe[0].get_identity ();
            msg_ = new Msg(identity.size());
            msg_.put(identity.data());
            msg_.set_flags (Msg.more);
            identity_sent = true;
        }

        return msg_;
    }
    
    //  Rollback any message parts that were sent but not yet flushed.
    protected void rollback () {
        
        if (current_out != null) {
            current_out.rollback ();
            current_out = null;
            more_out = false;
        }
    }
    
    @Override
    protected boolean xhas_in ()
    {
        //  If we are in the middle of reading the messages, there are
        //  definitely more parts available.
        if (more_in)
            return true;

        //  We may already have a message pre-fetched.
        if (prefetched)
            return true;

        //  Try to read the next message.
        //  The message, if read, is kept in the pre-fetch buffer.
        Pipe[] pipe = new Pipe[1];
        prefetched_msg = fq.recvpipe (pipe);
        if (pipe[0] == null)
            return false;

        //  Identity is not expected
        assert ((prefetched_msg.flags () & Msg.identity) == 0);

        Blob identity = pipe[0].get_identity ();
        prefetched_id = new Msg(identity.size());
        prefetched_id.put(identity.data());
        prefetched_id.set_flags (Msg.more);

        prefetched = true;
        identity_sent = false;

        return true;
    }

    @Override
    protected boolean xhas_out ()
    {
        //  In theory, ROUTER socket is always ready for writing. Whether actual
        //  attempt to write succeeds depends on whitch pipe the message is going
        //  to be routed to.
        return true;
    }

    private boolean identify_peer (Pipe pipe_)
    {
        Blob identity;

        Msg msg = pipe_.read ();
        if (msg == null)
            return false;

        if (msg.size () == 0) {
            //  Fall back on the auto-generation
            ByteBuffer buf = ByteBuffer.allocate(5);
            buf.put((byte)0);
            buf.putInt (next_peer_id++);
            buf.flip();
            identity = new Blob(buf);
            //msg.close ();
        }
        else {
            identity = new Blob(msg.data ());
            //msg.close ();

            //  Ignore peers with duplicate ID.
            if (outpipes.containsKey(identity))
                return false;
        }

        pipe_.set_identity (identity);
        //  Add the record into output pipes lookup table
        Outpipe outpipe = new Outpipe(pipe_, true);
        outpipes.put (identity, outpipe);

        return true;
    }


}