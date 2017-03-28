# Z-Queen
Java-Aio network transfer library base on LMAX-Disruptor

## Log output
`-Djava.util.logging.config.file=./config/LogConfig.properties`

## Z-Queen inner exchange command use Z-Protocol
### Protocol-Head:
[websocket](http://www.ietf.org/rfc/rfc6455.txt )
### Protocol-Command:
      0                   1                   2                   3
      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     +-+-+-+-+-------+---------------+-------------------------------+
     |T|R|P|C|version|  CMD-ID       |    MSG-ID(optional)           |
     |Y|S|R|R| (4)   |    (8)        |     (64)                      |
     |P|V|S|P|       |               |    	                         |
     |E| | |T|       |               |                               |
     +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
     |                            MSG-ID                             |
     + - - - - - - - - - - - - - - - +-------------------------------+
     |charset|Serial |                                               |
     |  (4)  |type(4)|        Payload Data                           |
     +-------------------------------- - - - - - - - - - - - - - - - +
     :                     Payload Data continued ...                :
     + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
     |                              CRC32                            |
     +---------------------------------------------------------------+
    
### Detail:
**Type:** 1bit 0x0 Direct        0x1 Cluster

**RSV:**  1bit 0x0 MSG_ID        0x1 no MSG_ID

**PRS:**  1bit 0x0 no compress   0x1 compress

**CRPT:** 1bit 0x0 plain         0x1 cipher

**Version:** 4bit 1~0xF

**CMD-ID:** 8bit unsigned byte 1-255

**MSG-ID:** 64bit  16bit clusterId  27bit timestamp(second) 21bit sequence

**Charset:** 4bit 

```
ASCII       = 0x00
UTF_8       = 0x01
UTF_8_NB    = 0x02
UTC_BE      = 0x03
UTC_LE      = 0x04
GBK         = 0x05
GB2312      = 0x06
GB18030     = 0x07
ISO_8859_1  = 0x08
ISO_8859_15 = 0x09
……
```
**Serial-Type:** 4bit payload serializion formatter 0x0 binary 0x1 proxy－transparent 0x2 JSON 0x3 XML

**CRC32:** CRC32（payload-data）

**Websoket-payload-length:** 7(no MSG-ID) | 15(MSG-ID) + Payload-data-length
