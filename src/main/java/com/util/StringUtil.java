package com.util;


import com.ddl.HotWord;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class StringUtil {

    private static String SEP_COMMA = ",";


    public static boolean stringIsBlank(String word) {
        boolean flag = true;
        if (null == word || "".equals(word.trim())) {
            return flag;
        } else {
            flag = false;
        }
        return flag;
    }

    /**
     *  返回json数组里面的所有id值
     * @param jsonArray
     * @return
     * @throws Exception
     */
    public static String jsonArrayToString(String jsonArray) throws Exception {
        if (stringIsBlank(jsonArray)) {
            return jsonArray;
        }
        StringBuffer sb = new StringBuffer();
        int num = 0;
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        HotWord[] list = mapper.readValue(jsonArray, HotWord[].class);

        for (HotWord word : list
        ) {

            if (num == 0) {
                sb.append(word.getId());
            } else {
                sb.append(SEP_COMMA).append(word.getId());
            }
            num++;

        }

        return sb.toString();

    }

    public static void main(String[] args) throws Exception {

        String jsonArray = "[{\"id\":\"jack\",\"producer_type\":\"1\"},{\"id\":\"jason\",\"producer_type\":\"2\"},{\"id\":\"lucy\",\"producer_type\":\"3\"}]";
        System.out.println(jsonArray);
        String result = StringUtil.jsonArrayToString(jsonArray);
        System.out.println(result);


    }


}

