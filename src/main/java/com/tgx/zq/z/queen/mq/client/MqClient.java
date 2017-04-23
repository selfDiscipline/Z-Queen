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

package com.tgx.zq.z.queen.mq.client;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import com.tgx.zq.z.queen.io.ws.protocol.bean.mq.X80_MqTopicReg;
import com.tgx.zq.z.queen.mq.MqMode;

/**
 * @author William.d.zk
 */
public class MqClient
        extends
        InnerMqClient
{
    private final Map<Integer, MqMode> regMap = new ConcurrentSkipListMap<>();

    public MqClient(String... start_argv) throws IOException {
        super(start_argv);
    }

    public void registerMqTopic(MqChannel channel, int command, MqMode mode) {
        MqMode preMode = regMap.put(command, mode);
        /* Idempotent register channel and mode */
        if (preMode == null || !preMode.equals(mode)) {
            channel.registerTopic(command, mode);
            registerChannel(channel);
            X80_MqTopicReg x80 = new X80_MqTopicReg();
            x80.channel = channel.getChannelId();
            x80.topicKeys = new short[] { (short) (mode.ordinal() << 8 | command) };
            publish(x80);
        }
    }

}
