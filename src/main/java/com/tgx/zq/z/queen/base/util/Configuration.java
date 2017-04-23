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
package com.tgx.zq.z.queen.base.util;

import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class Configuration
{

    /**
     * 读取配置文件信息
     * 
     * @param name
     *            读取节点名
     * @param fileName
     *            文件名
     * @return 读取的节点值
     */
    public static String readConfigString(String name, String fileName) throws MissingResourceException, ClassCastException {
        ResourceBundle rb = ResourceBundle.getBundle(fileName);
        return rb.getString(name);
    }

    public static boolean readConfigBoolean(String name, String fileName) throws MissingResourceException, ClassCastException {
        ResourceBundle rb = ResourceBundle.getBundle(fileName);
        return Boolean.parseBoolean(rb.getString(name));
    }

    public static int readConfigInteger(String name, String fileName) throws MissingResourceException,
                                                                      ClassCastException,
                                                                      NumberFormatException {
        ResourceBundle rb = ResourceBundle.getBundle(fileName);
        return Integer.parseInt(rb.getString(name));
    }

    public static int readConfigHexInteger(String name, String fileName) throws MissingResourceException,
                                                                         ClassCastException,
                                                                         NumberFormatException {
        ResourceBundle rb = ResourceBundle.getBundle(fileName);
        return Integer.parseInt(rb.getString(name), 16);
    }

    public static long readConfigHexLong(String name, String fileName) throws MissingResourceException,
                                                                       ClassCastException,
                                                                       NumberFormatException {
        ResourceBundle rb = ResourceBundle.getBundle(fileName);
        return Long.parseLong(rb.getString(name), 16);
    }

    public static long readConfigLong(String name, String fileName) throws MissingResourceException,
                                                                    ClassCastException,
                                                                    NumberFormatException {
        ResourceBundle rb = ResourceBundle.getBundle(fileName);
        return Long.parseLong(rb.getString(name));
    }

    public static double readConfigDouble(String name, String fileName) throws MissingResourceException,
                                                                        ClassCastException,
                                                                        NumberFormatException {
        ResourceBundle rb = ResourceBundle.getBundle(fileName);
        return Double.parseDouble(rb.getString(name));
    }

    public static float readConfigFloat(String name, String fileName) throws MissingResourceException,
                                                                      ClassCastException,
                                                                      NumberFormatException {
        ResourceBundle rb = ResourceBundle.getBundle(fileName);
        return Float.parseFloat(rb.getString(name));
    }

    public static long readConfigStorage(String name, String fileName) throws MissingResourceException,
                                                                       ClassCastException,
                                                                       NumberFormatException {
        ResourceBundle rb = ResourceBundle.getBundle(fileName);
        return IoUtil.readStorage(rb.getString(name));
    }
}
