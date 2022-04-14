package scaffolding;

import io.muserver.Mutils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.zip.GZIPInputStream;

public class ByteUtils {
    public static byte[] decompress(byte[] compressedContent) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Mutils.copy(new GZIPInputStream(new ByteArrayInputStream(compressedContent)), out, 8192);
        return out.toByteArray();
    }

    public static byte[] randomBytes(int len) {
        byte[] res = new byte[len];
        Random rng = new Random();
        rng.nextBytes(res);
        return res;
    }
}
