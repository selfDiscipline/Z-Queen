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

package com.tgx.zq.z.queen.io.inf;

import java.nio.channels.CompletionHandler;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.ShutdownChannelGroupException;
import java.nio.channels.WritePendingException;
import java.util.concurrent.RejectedExecutionException;

/**
 * @author William.d.zk
 */
public interface IPipeWrite
{
    default <C extends IContext> IProtocol filterWrite(IProtocol output, IFilterChain<C> filterChain, C context) {
        IFilter.ResultType resultType;
        IFilterChain<C> previousFilter = filterChain.getChainTail();
        while (previousFilter != null) {
            resultType = previousFilter.preEncode(context, output);
            switch (resultType) {
                case ERROR:
                case NEED_DATA:
                    return null;
                case NEXT_STEP:
                    output = previousFilter.encode(context, output);
                    break;
                case HANDLED:
                case IGNORE:
                    break;
            }
            previousFilter = previousFilter.getPrevious();
        }
        return output;
    }

    default <C extends IContext> Throwable sessionWrite(IProtocol output,
                                                        IFilterChain<C> filterChain,
                                                        ISession session,
                                                        CompletionHandler<Integer, ISession> writeHandler) {
        if (output == null || filterChain == null || session == null || writeHandler == null) return new NullPointerException();
        @SuppressWarnings("unchecked")
        IProtocol out = filterWrite(output, filterChain, (C) session.getContext());
        if (out != null && out instanceof IPoS) {
            try {
                session.write((IPoS) out, writeHandler);
            }
            catch (WritePendingException |
                   NotYetConnectedException |
                   ShutdownChannelGroupException |
                   RejectedExecutionException e) {
                return e;
            }
        }
        else if (out != null) return new UnsupportedOperationException();
        return null;
    }
}
