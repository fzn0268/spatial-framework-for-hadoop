package com.esri.hadoop.hive;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class HexUtils {
    static final Log LOG = LogFactory.getLog(HexUtils.class.getName());
    static BytesWritable hexTextToBytesWritable(Text geomrefhex) {
        if (geomrefhex == null || geomrefhex.getLength() == 0) {
            LogUtils.Log_ArgumentsNull(LOG);
            return null;
        }
        String geomrefstr = geomrefhex.toString();
        try {
            return new BytesWritable(Hex.decodeHex(geomrefstr.toCharArray()));
        } catch (DecoderException e) {
            LogUtils.Log_InvalidText(LOG, geomrefstr);
            return null;
        }
    }

    static Text bytesWritableToHexText(BytesWritable geomref) {
        if (geomref == null || geomref.getLength() == 0) {
            LogUtils.Log_ArgumentsNull(LOG);
            return null;
        }
        return new Text(Hex.encodeHexString(geomref.getBytes()));
    }

    static Text bytesToHexText(byte[] byteArr) {
        if (byteArr == null || byteArr.length == 0) {
            LogUtils.Log_ArgumentsNull(LOG);
            return null;
        }
        return new Text(Hex.encodeHexString(byteArr));
    }
}
