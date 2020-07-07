package cn.ljq2.web;

import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class WebParser {
    public static SimpleDateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
    public static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
    public static WebBean maper(String line) {
        WebBean webBean = new WebBean();
        String[] split = line.split(" ");
        if (split.length > 11) {
            webBean.setRemote_addr(split[0]);
            webBean.setRemote_user(split[1]);
            String date = formatDate(split[3].substring(1));
            if (date == null || date.equals("")) date = "-invalid_time-";
            webBean.setTime_local(date);
            webBean.setRequest(split[7]);
            webBean.setStatus(split[8]);
            webBean.setBody_bytes_sent(split[9]);
            webBean.setHttp_referer(split[10]);
            if (split.length > 11) {
                StringBuilder sb = new StringBuilder();
                for (int i = 11; i < split.length; i++) {
                    sb.append(split[i]);
                }
                webBean.setHttp_user_agent(sb.toString());
            }
            if (Integer.parseInt(webBean.getStatus()) > 400) {
                webBean.setValid(false);
            }
            if (webBean.getTime_local().equals("-invalid_time-")) {
                webBean.setValid(false);
            }
        } else {
            webBean = null;
        }
        return webBean;
    }

    public static String formatDate(String time_local) {
        try {
            return df2.format(df1.parse(time_local));
        } catch (Exception e) {
            return null;
        }
    }
}
