package scaffolding.testrouter;

import io.muserver.*;

import javax.ws.rs.NotFoundException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class FavIconHandler implements MuHandler {

    private final byte[] favicon;

    private FavIconHandler(byte[] favicon) {
        this.favicon = favicon;
    }

    public static FavIconHandler fromClassPath(String iconPath) throws IOException {
        byte[] bytes;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             InputStream fav = FavIconHandler.class.getResourceAsStream(iconPath)) {
            Mutils.copy(fav, baos, 8192);
            bytes = baos.toByteArray();
        }
        return new FavIconHandler(bytes);
    }

    @Override
    public boolean handle(MuRequest req, MuResponse resp) throws Exception {
        String target = req.uri().getPath();
        if (target.equals("/favicon.ico") && req.method() == Method.GET) {
            if (favicon == null) {
                throw new NotFoundException();
            }
            resp.status(200);
            resp.contentType(ContentTypes.IMAGE_X_ICON);
            resp.headers().set(HeaderNames.CONTENT_LENGTH, favicon.length);
            resp.headers().set(HeaderNames.CACHE_CONTROL, "max-age=360000,public");
            resp.outputStream().write(favicon);
            return true;
        }
        return false;
    }
}
