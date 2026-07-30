package Study.version;

/**
 * @author lzk
 * @date 2026/7/30 20:34
 * @description 协议的版本号
 */
public enum Version {
    V1(0),;

    private final int versionNum;

    Version(int versionNum) {
        this.versionNum = versionNum;
    }

    public int getVersionNum() {
        return versionNum;
    }
}
