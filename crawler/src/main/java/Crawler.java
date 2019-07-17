import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONObject;
import com.github.kevinsawicki.http.HttpRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.PriorityQueue;

/**
 * @Author: liuhang
 * @CreateTime: 2019-07-04 18:43
 */
public class Crawler {
    public static final byte m = 15;

    public static void main(String[] args) {
        String sign = getSign();
        System.out.println(sign);
//        String url = "https://app.shejiaolink.cn/home/getGoodsCate?cateType=&secondCate=&sortName=&sortType=&customerId=&pageSize=10&page={0}&status=";
//        HttpRequest httpRequest = new HttpRequest(url, "post");
//        httpRequest.

    }
    /**
     * 向指定 URL 发送POST方法的请求
     *
     * @param url
     *            发送请求的 URL
     * @param param
     *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
     * @return 所代表远程资源的响应结果
     */
    public static String sendPost(String url, String param) {
        PrintWriter out = null;
        BufferedReader in = null;
        String result = "";
        try {
            URL realUrl = new URL(url);
            // 打开和URL之间的连接
            URLConnection conn = realUrl.openConnection();
            // 设置通用的请求属性
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.setRequestProperty("user-agent",
                    "GoShare2/5.2.4 (iPhone; iOS 12.2; Scale/2.00)");
            // 发送POST请求必须设置如下两行
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // 获取URLConnection对象对应的输出流
            out = new PrintWriter(conn.getOutputStream());
            // 发送请求参数
            out.print(param);
            // flush输出流的缓冲
            out.flush();
            // 定义BufferedReader输入流来读取URL的响应
            in = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送 POST 请求出现异常！"+e);
            e.printStackTrace();
        }
        //使用finally块来关闭输出流、输入流
        finally{
            try{
                if(out!=null){
                    out.close();
                }
                if(in!=null){
                    in.close();
                }
            }
            catch(IOException ex){
                ex.printStackTrace();
            }
        }
        return result;
    }

    public static String getSign(){
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("deviceID","AD921D60486366258809553A3DB49A4A");
        jsonObject.put("deviceName","Google|Android SDK built for x86");
        jsonObject.put("appPlatform","Android");
        jsonObject.put("platformType","Phone");
        jsonObject.put("resolution","1794*1080");
        jsonObject.put("network","WIFI");
        JSONArray jsonArray = new JSONArray();
        //cf3bc6c940042c262b543423bb5e10ab|1562292339|1562291795
        //1af9eeefe3a98fab003b4afa4b7fc746|1562307604|1562307585
        jsonArray.put(new JSONObject().put("id","5213CD39D3BFFBD0A2FFEF67A7FDA13F"));
        jsonArray.put(new JSONObject().put("startTime","1562323798049"));
        jsonArray.put(new JSONObject().put("endTime","1562324573294"));
        jsonArray.put(new JSONObject().put("user_id","0"));
        jsonArray.put(new JSONObject().put("os","10"));
        jsonObject.put("sessions",jsonArray);
        String s = jsonObject.toString();
        HashMap<String, String> hashMap = new HashMap<String, String>();
        hashMap.put("data", new String(Base64.encodeBase64(s.getBytes(), false)));
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb = new StringBuilder();
        for (String str : hashMap.keySet()) {
            sb.append("&");
            sb.append(str);
            sb.append("=");
            sb.append((String) hashMap.get(str));
        }
        if (sb.length() > 0) {
            sb.delete(0, 1);
        }
        sb1.append(sb.toString().trim());
        sb1.append("&token=");
        sb1.append("123456");
        return MD5(sb1.toString());
    }

    public static final String MD5(String str) {
        byte[] digest;
        char[] cArr = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
        try {
            byte[] bytes = str.getBytes();
            MessageDigest instance = MessageDigest.getInstance("MD5");
            instance.update(bytes);
            char[] cArr2 = new char[32];
            int i = 0;
            for (byte b : instance.digest()) {
                int i2 = i + 1;
                cArr2[i] = cArr[(b >>> 4) & 15];
                i = i2 + 1;
                cArr2[i2] = cArr[b & m];
            }
            return new String(cArr2);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}
