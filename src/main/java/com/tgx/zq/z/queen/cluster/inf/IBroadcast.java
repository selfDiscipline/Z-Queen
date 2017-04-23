
/*
 *  MIT License
 *
 *  Copyright (c) 2016~2017 Z-Chess
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 *
 */

package com.tgx.zq.z.queen.cluster.inf;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.ws.protocol.Command;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;

public interface IBroadcast<C extends WsContext>
{

    default List<ICommand> sendDirect(Command<C> cmd, long nodeId) {
        List<ICommand> wList = new LinkedList<>();
        sendDirect(wList, cmd, nodeId);
        return wList;
    }

    void sendDirect(List<ICommand> commandList, Command<C> cmd, long nodeId);

    default List<ICommand> sendExcept(Command<C> cmd, long nodeId) {
        List<ICommand> wList = new LinkedList<>();
        sendExcept(wList, cmd, nodeId);
        return wList;
    }

    void sendExcept(List<ICommand> commandList, Command<C> cmd, long nodeId);

    default List<ICommand> sendAll(Command<C> cmd) {
        List<ICommand> wList = new LinkedList<>();
        sendAll(wList, cmd);
        return wList;
    }

    default List<ICommand> send(Command<C> cmd, Collection<Long> nodes) {
        List<ICommand> wList = new LinkedList<>();
        sendCollection(wList, cmd, nodes);
        return wList;
    }

    void sendAll(List<ICommand> wList, Command<C> cmd);

    default void sendCollection(List<ICommand> wList, Command<C> cmd, Collection<Long> nodeIds) {
        for (long nodeId : nodeIds)
            sendDirect(wList, cmd, nodeId);
    }

    default void sendCollection(List<ICommand> wList, Command<C> cmd, long... nodeIds) {
        for (long nodeId : nodeIds)
            sendDirect(wList, cmd, nodeId);
    }

    void flush(List<ICommand> wList);

}
