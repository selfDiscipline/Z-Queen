/*
 * MIT License
 *
 * Copyright (c) 2017 Z-Chess
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.tgx.zq.z.queen.mq.client;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import com.tgx.zq.z.queen.client.socket.aio.AioSingleClient;
import com.tgx.zq.z.queen.io.bean.XF000_NULL;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.ws.protocol.bean.mq.X80_MqTopicReg;
import com.tgx.zq.z.queen.io.ws.protocol.bean.mq.X82_MqPush;
import com.tgx.zq.z.queen.mq.MqMode;

public abstract class MqChannel
        implements
        AioSingleClient.IChannelLogic
{

    private final ConcurrentSkipListMap<Integer, MqMode> _RegisterMap = new ConcurrentSkipListMap<>();

    @Override
    public final ICommand onConnectedServer() {
        if (_RegisterMap.isEmpty()) return XF000_NULL.INSTANCE;
        X80_MqTopicReg x80 = new X80_MqTopicReg();
        x80.channel = getChannelId();
        x80.topicKeys = new short[_RegisterMap.size()];
        int index = 0;
        for (Map.Entry<Integer, MqMode> entry : _RegisterMap.entrySet()) {
            int topic = entry.getKey();
            MqMode mode = entry.getValue();
            x80.topicKeys[index++] = (short) (mode.ordinal() << 8 | topic);
        }
        return x80;
    }

    void registerTopic(int command, MqMode mode) {
        _RegisterMap.put(command, mode);
    }

    @Override
    public Collection<ICommand> handle(ICommand inCommand) {
        if (inCommand.getSerialNum() == X82_MqPush.COMMAND) {
            X82_MqPush x82 = (X82_MqPush) inCommand;

        }
        return null;
    }

}
