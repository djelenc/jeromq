package com.david;

import org.zeromq.ZMQ;

public class MyCase {
    public static void main(String[] args) {
        final ZMQ.Context ctx = ZMQ.context(1);
        final ZMQ.Socket req = ctx.socket(ZMQ.DEALER);
        req.connect("tcp://localhost:9999");

        final ZMQ.Socket rep = ctx.socket(ZMQ.DEALER);
        rep.bind("tcp://*:9999");

        req.send("Hello");
        System.out.println("Sent");

        final String repIn = rep.recvStr();
        System.out.println("REQUEST: " + repIn);
        final String repOut = repIn.toUpperCase() + " WORLD!";

        rep.send(repOut);

        System.out.println("RESPONSE: " + req.recvStr());
    }
}
