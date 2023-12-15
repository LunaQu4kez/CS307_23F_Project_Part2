package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Authentication {
    private static final boolean USE_HASH = false;
    public static final long[] BASE = {1, 257, 66049, 197425, 406721, 718570, 123642, 318804, 143934, 290983, 333948, 890223, 198397, 656525, 955245, 131883, 339595, 244356, 933685, 882401};
    public static final long MOD_A = 1048573;
    public static final long MOD_B = 4194319;


    public static boolean authentication(AuthInfo auth, DataSource dataSource) {
        try (Connection conn = dataSource.getConnection()) {
            String sql = "select password, qq, wechat from user_auth where mid = ?";
            long mid = auth.getMid();
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setLong(1, mid);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                rs.close();
                stmt.close();
                return false;
            }
            // true info(after hash)
            String pw0 = rs.getString(1);
            String qq0 = rs.getString(2);
            String wechat0 = rs.getString(3);
            // auth info(before hash)
            String pw = auth.getPassword();
            String qq = auth.getQq();
            String wechat = auth.getWechat();

            if ((pw == null || pw.equals("")) &&
                    (qq == null || qq.equals("")) &&
                    (wechat == null || wechat.equals(""))) {
                rs.close();
                stmt.close();
                return false;
            }

            if (pw != null && !pw.equals("") && !hash(pw, mid).equals(pw0)) {
                rs.close();
                stmt.close();
                return false;
            }
            if (qq != null && !qq.equals("") && qq.equals(qq0)) {
                rs.close();
                stmt.close();
                return false;
            }
            if (wechat != null && !wechat.equals("") && wechat.equals(wechat0)) {
                rs.close();
                stmt.close();
                return false;
            }
            rs.close();
            stmt.close();
            return true;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        }
    }

    public static String hash(String str, long mid) {
        if (!USE_HASH) return str;
        long result = str.charAt(0);
        for (int i = 1; i < str.length(); i++) result = (str.charAt(i) * BASE[i] % MOD_A + result) % MOD_A;
        return Long.toString(Long.parseLong(result +Long.toString(mid % MOD_A)) % MOD_B);
    }
}
