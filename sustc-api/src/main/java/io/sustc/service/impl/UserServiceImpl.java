package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Slf4j
public class UserServiceImpl implements UserService {

    @Autowired
    private DataSource dataSource;

    @Override
    public long register(RegisterUserReq req) {
        try (Connection conn = dataSource.getConnection()) {
            if (req.getPassword() == null || req.getName() == null || req.getSex() == null
                    || req.getPassword().equals("") || req.getName().equals("")){
                return -1;
            }
            if (req.getBirthday() != null && !req.getBirthday().isEmpty()){
                if (!BirthdayValid(req.getBirthday())){
                    return -1;
                }
            }

            PreparedStatement stmt;
            ResultSet rs;
            if (req.getQq() != null && !req.getQq().equals("")) {
                String sql2 = "select * from user_auth where qq = ?";
                stmt = conn.prepareStatement(sql2);
                stmt.setString(1, req.getQq());
                rs = stmt.executeQuery();
                if (rs.next()){
                    stmt.close();
                    rs.close();
                    return -1;
                }
            }
            if (req.getWechat() != null && !req.getWechat().equals("")) {
                String sql2 = "select * from user_auth where wechat = ?";
                stmt = conn.prepareStatement(sql2);
                stmt.setString(1, req.getWechat());
                rs = stmt.executeQuery();
                if (rs.next()){
                    stmt.close();
                    rs.close();
                    return -1;
                }
            }

            String sql3 = "select * from max_mid ";
            stmt = conn.prepareStatement(sql3);
            rs = stmt.executeQuery();
            rs.next();
            long new_mid = rs.getLong(1) + 1;

            String sql6 = "insert into user_info (mid, name, sex, birthday, level, sign, identity) "
                    + "values(?,?,?,?,?,?,?)";
            stmt = conn.prepareStatement(sql6);
            stmt.setLong(1, new_mid);
            stmt.setString(2, req.getName());
            if (req.getSex() == RegisterUserReq.Gender.MALE) {
                stmt.setString(3, "男");
            } else if (req.getSex() == RegisterUserReq.Gender.FEMALE) {
                stmt.setString(3, "女");
            } else {
                stmt.setString(3, "保密");
            }
            stmt.setString(4, req.getBirthday());
            stmt.setInt(5, 0);
            stmt.setString(6, req.getSign());
            stmt.setString(7, "user");
            stmt.executeUpdate();

            String sql5 = "insert into user_auth (mid, password, qq, wechat) " + "values(?,?,?,?)";
            stmt = conn.prepareStatement(sql5);
            stmt.setLong(1, new_mid);
            stmt.setString(2, Authentication.hash(req.getPassword(), new_mid));
            stmt.setString(3, req.getQq());
            stmt.setString(4,req.getWechat());
            stmt.executeUpdate();

            String sql4 = "update max_mid set max_mid = ?";
            stmt = conn.prepareStatement(sql4);
            stmt.setLong(1, new_mid);
            stmt.executeUpdate();

            stmt.close();
            rs.close();
            return new_mid;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long mid) {
        long auth_mid = Authentication.authentication(auth, dataSource);
        if (auth_mid == 0){
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            String sql1 = "select * from user_auth where mid = ?";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            stmt.setLong(1, mid);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()){
                rs.close();
                stmt.close();
                return false;
            }

            String sql2 = "select identity from user_info where mid = ?";
            stmt = conn.prepareStatement(sql2);
            stmt.setLong(1, mid);
            rs = stmt.executeQuery();
            rs.next();
            String DeleIden = rs.getString(1);

            String sql3 = "select identity from user_info where mid = ?";
            stmt = conn.prepareStatement(sql3);
            stmt.setLong(1, auth_mid);
            rs = stmt.executeQuery();
            rs.next();
            String AuthIden = rs.getString(1);

            if (AuthIden.equals("user") && mid != auth_mid){
                rs.close();
                stmt.close();
                return false;
            }
            if (AuthIden.equals("superuser") && !DeleIden.equals("user") && mid != auth_mid){
                rs.close();
                stmt.close();
                return false;
            }
            String sql5 = "delete from user_auth where mid = ?";
            stmt = conn.prepareStatement(sql5);
            stmt.setLong(1, mid);
            stmt.executeUpdate();

            String sql4 = "delete from user_info where mid = ?";
            stmt = conn.prepareStatement(sql4);
            stmt.setLong(1, mid);
            int x = stmt.executeUpdate();
            rs.close();
            stmt.close();
            return x != 0;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeMid) {
        long auth_mid = Authentication.authentication(auth, dataSource);
        if (auth_mid == 0){
            return false;
        }
        if (auth_mid == followeeMid) {
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            String sql1 = "select * from user_auth where mid = ?";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            stmt.setLong(1, followeeMid);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()){
                rs.close();
                stmt.close();
                return false;
            }

            String sql2 = "select * from follow where up_mid = ? and fans_mid = ?";
            stmt = conn.prepareStatement(sql2);
            stmt.setLong(1, followeeMid);
            stmt.setLong(2, auth_mid);
            rs = stmt.executeQuery();

            if (!rs.next()){
                String sql3 = "insert into follow (up_mid, fans_mid) values (?, ?)";
                stmt = conn.prepareStatement(sql3);
                stmt.setLong(1, followeeMid);
                stmt.setLong(2, auth_mid);
                stmt.executeUpdate();
                rs.close();
                stmt.close();
                return true;
            }else {
                String sql4 = "delete from follow where up_mid = ? and fans_mid = ?";
                stmt = conn.prepareStatement(sql4);
                stmt.setLong(1, followeeMid);
                stmt.setLong(2, auth_mid);
                stmt.executeUpdate();
                rs.close();
                stmt.close();
                return false;
            }
        }catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public UserInfoResp getUserInfo(long mid) {
        try (Connection conn = dataSource.getConnection()) {
            String sql1 = "select coin from user_info where mid = ?";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            stmt.setLong(1, mid);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()){
                rs.close();
                stmt.close();
                return null;
            }
            int coin = rs.getInt(1);

            List<Long> data1 = new ArrayList<>();
            String sql2 = "select up_mid from follow where fans_mid = ?";
            stmt = conn.prepareStatement(sql2);
            stmt.setLong(1, mid);
            rs = stmt.executeQuery();
            while (rs.next()){
                long data = rs.getLong(1);
                data1.add(data);
            }
            long[] following = data1.stream().mapToLong(Long::longValue).toArray();

            List<Long> data2 = new ArrayList<>();
            String sql3 = "select fans_mid from follow where up_mid = ?";
            stmt = conn.prepareStatement(sql3);
            stmt.setLong(1, mid);
            rs = stmt.executeQuery();
            while (rs.next()){
                long data = rs.getLong(1);
                data2.add(data);
            }
            long[] follower = data2.stream().mapToLong(Long::longValue).toArray();

            List<String> data3 = new ArrayList<>();
            String sql4 = "select bv from view_video where mid = ?";
            stmt = conn.prepareStatement(sql4);
            stmt.setLong(1, mid);
            rs = stmt.executeQuery();
            while (rs.next()){
                String data = rs.getString(1);
                data3.add(data);
            }
            String[] watched = data3.toArray(new String[0]);

            List<String> data4 = new ArrayList<>();
            String sql5 = "select bv from like_video where mid = ?";
            stmt = conn.prepareStatement(sql5);
            stmt.setLong(1, mid);
            rs= stmt.executeQuery();
            while (rs.next()){
                String data = rs.getString(1);
                data4.add(data);
            }
            String[] liked = data4.toArray(new String[0]);

            List<String> data5 = new ArrayList<>();
            String sql6 = "select bv from fav_video where mid = ?";
            stmt = conn.prepareStatement(sql6);
            stmt.setLong(1, mid);
            rs = stmt.executeQuery();
            while (rs.next()){
                String data = rs.getString(1);
                data5.add(data);
            }
            String[] collected = data5.toArray(new String[0]);

            List<String> data6 = new ArrayList<>();
            String sql7 = "select bv from video_info where owner_mid = ?";
            stmt = conn.prepareStatement(sql7);
            stmt.setLong(1, mid);
            rs = stmt.executeQuery();
            while (rs.next()){
                String data = rs.getString(1);
                data6.add(data);
            }
            String[] posted = data6.toArray(new String[0]);

            rs.close();
            stmt.close();
            return new UserInfoResp(mid, coin, following, follower, watched, liked, collected, posted );
        }catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    public static boolean BirthdayValid(String birthday){
        Pattern pattern = Pattern.compile("(\\d{1,2})月(\\d{1,2})日");
        Matcher matcher = pattern.matcher(birthday);
        if (matcher.matches()) {
            int month = Integer.parseInt(matcher.group(1));
            int day = Integer.parseInt(matcher.group(2));
            if (month > 12 || day > 31 ) {
                return false;
            }
            if ((month == 4 || month == 6 || month == 9 || month == 11) && day > 30){
                return false;
            }
            if (month == 2 && day > 28){
                return false;
            }
            return true;
        }
        return false;
    }
}