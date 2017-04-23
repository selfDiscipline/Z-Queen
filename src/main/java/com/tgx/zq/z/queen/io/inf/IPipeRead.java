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

import com.tgx.zq.z.queen.base.util.IoUtil;

/**
 * @author William.d.zk
 */
public interface IPipeRead
{

    default <C extends IContext> ICommand[] filterRead(IProtocol input, IFilterChain<C> filterChain, C context) {
        IFilterChain<C> headerFilter = filterChain.getChainHead();
        ICommand commands[] = null;
        IFilter.ResultType resultType;
        IProtocol _Pd = input;
        for (IFilterChain<C> nextFilter = headerFilter;; nextFilter = headerFilter, _Pd = input) {
            Chain:
            while (nextFilter != null) {
                resultType = nextFilter.preDecode(context, _Pd);
                switch (resultType) {
                    case ERROR:
                        return null;
                    case NEED_DATA:
                        return commands;
                    case NEXT_STEP:
                        _Pd = nextFilter.decode(context, _Pd);
                        break;
                    case HANDLED:
                        ICommand cmd = (ICommand) nextFilter.decode(context, _Pd);
                        if (cmd != null) {
                            if (commands == null) commands = new ICommand[] { cmd };
                            else {
                                ICommand[] nCmd = new ICommand[commands.length + 1];
                                IoUtil.addArray(commands, nCmd, cmd);
                                commands = nCmd;
                            }
                        }
                        break Chain;
                    case IGNORE:
                        break;
                }
                nextFilter = nextFilter.getNext();
            }
        }
    }
}
