package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Set;

@Service
@Slf4j
public class VideoServiceImpl implements VideoService {

    @Autowired
    private DataSource dataSource;

    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req) {
        try (Connection conn = dataSource.getConnection()) {
            if (!authentication(auth, conn)) {
                return null;
            }

            Timestamp ts = new Timestamp(System.currentTimeMillis());
            if (req.getTitle() == null || req.getTitle().equals("") || req.getDuration() < 10
                || ts.after(req.getPublicTime())) {
                return null;
            }
            String sql1 = "select * from video_info where title = ? and owner_mid = ?";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            stmt.setString(1, req.getTitle());
            stmt.setLong(2, auth.getMid());
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                rs.close();
                stmt.close();
                return null;
            }

            String sql2 = "select count(*) from video_info";
            stmt = conn.prepareStatement(sql2);
            rs = stmt.executeQuery();
            rs.next();
            long num = rs.getLong(1);
            String bv = "BV" + (num + 1);


            String sql3 = "insert into video_info (bv, title, duration, description, owner_mid, commit_time, public_time) " +
                    "values (?,?,?,?,?,?,?)";
            stmt = conn.prepareStatement(sql3);
            stmt.setString(1, bv);
            stmt.setString(2, req.getTitle());
            stmt.setFloat(3, req.getDuration());
            stmt.setString(4, req.getDescription());
            stmt.setLong(5, auth.getMid());
            stmt.setTimestamp(6, ts);
            stmt.setTimestamp(7, req.getPublicTime());
            stmt.executeUpdate();

            rs.close();
            stmt.close();
            return bv;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        return false;
    }

    @Override
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        return null;
    }

    @Override
    public double getAverageViewRate(String bv) {
        return 0;
    }

    @Override
    public Set<Integer> getHotspot(String bv) {
        return null;
    }

    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        return false;
    }

    private boolean authentication(AuthInfo auth, Connection conn) {
        try {
            String sql = "select password, qq, wechat from user_auth where mid = ?";
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setLong(1, auth.getMid());
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                rs.close();
                stmt.close();
                return false;
            }
            // true info
            String pw0 = rs.getString(1);
            String qq0 = rs.getString(2);
            String wechat0 = rs.getString(3);
            // auth info
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
            if (pw != null && !pw.equals("") && !pw.equals(pw0)){
                rs.close();
                stmt.close();
                return false;
            }
            if (qq != null && !qq.equals("") && !qq.equals(qq0)){
                rs.close();
                stmt.close();
                return false;
            }
            if (wechat != null && !wechat.equals("") && !wechat.equals(wechat0)){
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
}
