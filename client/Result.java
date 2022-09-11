package client;

public class Result {
    private final ResultType resultType;
    private int count = 0;
    private StringBuffer stringBuffer = new StringBuffer();

    public static final String VM_HOME_ADDRESS = "/home/maojunx2/";
    public static final String VM_LOG_ADDRESS = "mp1/test.log";

    public Result(ResultType resultType) {
        this.resultType = resultType;
    }

    public void add(String result) {
        if (resultType == ResultType.Integer) {
            result = result.replace(VM_HOME_ADDRESS + VM_LOG_ADDRESS + ":", "").replace("\n", "");
            this.count += Integer.parseInt(result);
        } else {
            this.stringBuffer.append(result);
        }
    }

    public String getResult() {
        if (resultType == ResultType.Integer) {
            return this.count + "";
        } else {
            return this.stringBuffer.toString();
        }
    }
}
