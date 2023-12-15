package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Authentication {
    private static final boolean USE_HASH = false;

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

            if (pw != null && !pw.equals("") && !hash(pw, mid).equals(pw0)){
                rs.close();
                stmt.close();
                return false;
            }
            if (qq != null && !qq.equals("") && qq.equals(qq0)){
                rs.close();
                stmt.close();
                return false;
            }
            if (wechat != null && !wechat.equals("") && wechat.equals(wechat0)){
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
        if (!USE_HASH) {
            return str;
        } else {
            return str;
        }
    }
}
