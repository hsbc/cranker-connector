package scaffolding.testrouter;


public interface IPValidator {

    boolean allow(String ip);

    IPValidator AllowAll = ip -> true;
}
