package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;

import javax.sql.DataSource;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.commons.codec.digest.DigestUtils;

public class Authentication {
    private static final boolean USE_HASH = true;
    private static final boolean USE_MD5 = true;
    public static final long[] BASE = {1, 257, 66049, 197425, 406721, 718570, 123642, 318804, 143934, 290983, 333948, 890223, 198397, 656525, 955245, 131883, 339595, 244356, 933685, 882401};
    public static final long MOD_A = 1048573;
    public static final long MOD_B = 2147483647;
    public static final BigInteger MOD_C = new BigInteger("9223372036854775783");


    public static long authentication(AuthInfo auth, DataSource dataSource) {
        boolean b1 = auth.getMid() != 0 && auth.getPassword() != null && !auth.getPassword().equals("");
        boolean b2 = auth.getQq() != null && !auth.getQq().equals("");
        boolean b3 = auth.getWechat() != null && !auth.getWechat().equals("");

        try (Connection conn = dataSource.getConnection()) {
            if (b1) {
                String sql = "select password from user_auth where mid = ?";
                long mid = auth.getMid();
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.setLong(1, mid);
                ResultSet rs = stmt.executeQuery();
                if (!rs.next()) {
                    rs.close();
                    stmt.close();
                    return 0;
                }
                String pw = rs.getString(1);
                return pw.equals(hash(auth.getPassword(), mid)) ? mid : 0;
            } else if (b2) {
                String sql = "select mid from user_auth where qq = ?";
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.setString(1, auth.getQq());
                ResultSet rs = stmt.executeQuery();
                if (!rs.next()) {
                    rs.close();
                    stmt.close();
                    return 0;
                } else {
                    return rs.getLong(1);
                }
            } else {
                String sql = "select mid from user_auth where wechat = ?";
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.setString(1, auth.getWechat());
                ResultSet rs = stmt.executeQuery();
                if (!rs.next()) {
                    rs.close();
                    stmt.close();
                    return 0;
                } else {
                    return rs.getLong(1);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return 0;
        }
    }

    public static String hash(String str, long mid) {
        if (!USE_HASH) return str;
        if (USE_MD5) return MD5SaltHash(str, mid);
        long result = str.charAt(0);
        for (int i = 1; i < str.length(); i++) result = (str.charAt(i) * BASE[i] % MOD_A + result) % MOD_A;
        return Long.toString(Long.parseLong(result + Long.toString(mid % MOD_A)) % MOD_B);
    }

    public static String MD5SaltHash(String str, long mid) {
        return new BigInteger(DigestUtils.md5Hex(str) + mid % MOD_A, 16).mod(MOD_C).toString();
    }
}
