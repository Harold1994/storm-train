package com.harold.storm.intergrate;



import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;

public class DateUtil {
    private static DateUtil instance;
    private DateUtil() {}
    public static synchronized DateUtil getInstance() {
        if (instance == null)
            instance = new DateUtil();
        return instance;
    }

    public long getTime(String time) throws ParseException {
        long ans = DateUtils.parseDate(time.substring(1, time.length() - 1), "yyyy-MM-dd HH:mm:ss").getTime();
        return ans;
    }
}
