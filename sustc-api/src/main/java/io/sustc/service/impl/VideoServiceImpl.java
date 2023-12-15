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
import java.util.ArrayList;
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
        if (!Authentication.authentication(auth, dataSource)) {
            return null;
        }

        try (Connection conn = dataSource.getConnection()) {
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

//            String sql2 = "select count(*) from video_info";
//            stmt = conn.prepareStatement(sql2);
//            rs = stmt.executeQuery();
//            rs.next();
//            long num = rs.getLong(1);
//            String bv = "BV" + (num + 1);
            String bv = generateBV();

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
        if (!Authentication.authentication(auth, dataSource)) {
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            String sql1 = "select owner_mid from video_info where bv = ?";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                rs.close();
                stmt.close();
                return false;
            }
            long owner_mid = rs.getLong(1);
            stmt.close();
            rs.close();

            String sql2 = "select identity from user_info where mid = ?";
            stmt = conn.prepareStatement(sql2);
            stmt.setLong(1, auth.getMid());
            rs = stmt.executeQuery();
            rs.next();
            String identity = rs.getString(1);
            if (!identity.equals("superuser") && owner_mid != auth.getMid()) {
                stmt.close();
                rs.close();
                return false;
            }

            String sql3 = "delete from video_info where bv = ?";
            stmt = conn.prepareStatement(sql3);
            stmt.setString(1, bv);
            return stmt.executeUpdate() != 0;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
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
        if (!Authentication.authentication(auth, dataSource)) {
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            String sql1 = "select owner_mid, public_time, can_see from video_info where bv = ?";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                rs.close();
                stmt.close();
                return false;
            }
            long ownerMid = rs.getLong(1);
            Timestamp publicTime = rs.getTimestamp(2);
            boolean canSee = rs.getBoolean(3);

            String sql2 = "select identity, coin from user_info where mid = ?";
            stmt = conn.prepareStatement(sql2);
            stmt.setLong(1, auth.getMid());
            rs = stmt.executeQuery();
            rs.next();
            String identity = rs.getString(1);
            int coin = rs.getInt(2);

            if (auth.getMid() == ownerMid) {
                rs.close();
                stmt.close();
                return false;
            }
            if ((identity.equals("user") && canSee && new Timestamp(System.currentTimeMillis()).after(publicTime)) ||
                    (identity.equals("superuser"))) {
                String sql3 = "select * from coin_video where mid = ? and bv = ?";
                stmt = conn.prepareStatement(sql3);
                stmt.setLong(1, auth.getMid());
                stmt.setString(2, bv);
                rs = stmt.executeQuery();
                if (rs.next() || coin == 0) {
                    rs.close();
                    stmt.close();
                    return false;
                }

                String sql4 = "insert into coin_video (bv, mid) values (?, ?)";
                stmt = conn.prepareStatement(sql4);
                stmt.setString(1, bv);
                stmt.setLong(2, auth.getMid());
                int x =  stmt.executeUpdate();
                if (x == 0) {
                    rs.close();
                    stmt.close();
                    return false;
                }

                String sql5 = "update user_info set coin = ? where mid = ?";
                stmt = conn.prepareStatement(sql5);
                stmt.setInt(1, coin - 1);
                stmt.setLong(2, auth.getMid());
                stmt.executeUpdate();
                return true;
            } else {
                rs.close();
                stmt.close();
                return false;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        if (!Authentication.authentication(auth, dataSource)) {
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            String sql1 = "select owner_mid, public_time, can_see from video_info where bv = ?";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                rs.close();
                stmt.close();
                return false;
            }
            long ownerMid = rs.getLong(1);
            Timestamp publicTime = rs.getTimestamp(2);
            boolean canSee = rs.getBoolean(3);

            String sql2 = "select identity from user_info where mid = ?";
            stmt = conn.prepareStatement(sql2);
            stmt.setLong(1, auth.getMid());
            rs = stmt.executeQuery();
            rs.next();
            String identity = rs.getString(1);

            if (auth.getMid() == ownerMid) {
                rs.close();
                stmt.close();
                return false;
            }
            if ((identity.equals("user") && canSee && new Timestamp(System.currentTimeMillis()).after(publicTime)) ||
                    (identity.equals("superuser"))) {
                String sql3 = "select * from like_video where mid = ? and bv = ?";
                stmt = conn.prepareStatement(sql3);
                stmt.setLong(1, auth.getMid());
                stmt.setString(2, bv);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    rs.close();
                    stmt.close();
                    return false;
                }

                String sql4 = "insert into like_video (bv, mid) values (?, ?)";
                stmt = conn.prepareStatement(sql4);
                stmt.setString(1, bv);
                stmt.setLong(2, auth.getMid());
                return stmt.executeUpdate() != 0;
            } else {
                rs.close();
                stmt.close();
                return false;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        if (!Authentication.authentication(auth, dataSource)) {
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            String sql1 = "select owner_mid, public_time, can_see from video_info where bv = ?";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                rs.close();
                stmt.close();
                return false;
            }
            long ownerMid = rs.getLong(1);
            Timestamp publicTime = rs.getTimestamp(2);
            boolean canSee = rs.getBoolean(3);

            String sql2 = "select identity from user_info where mid = ?";
            stmt = conn.prepareStatement(sql2);
            stmt.setLong(1, auth.getMid());
            rs = stmt.executeQuery();
            rs.next();
            String identity = rs.getString(1);

            if (auth.getMid() == ownerMid) {
                rs.close();
                stmt.close();
                return false;
            }
            if ((identity.equals("user") && canSee && new Timestamp(System.currentTimeMillis()).after(publicTime)) ||
                    (identity.equals("superuser"))) {
                String sql3 = "select * from fav_video where mid = ? and bv = ?";
                stmt = conn.prepareStatement(sql3);
                stmt.setLong(1, auth.getMid());
                stmt.setString(2, bv);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    rs.close();
                    stmt.close();
                    return false;
                }

                String sql4 = "insert into fav_video (bv, mid) values (?, ?)";
                stmt = conn.prepareStatement(sql4);
                stmt.setString(1, bv);
                stmt.setLong(2, auth.getMid());
                return stmt.executeUpdate() != 0;
            } else {
                rs.close();
                stmt.close();
                return false;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private String generateBV() {
        try (Connection conn = dataSource.getConnection()) {
            String sql1 = "select bv from video_info";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            ResultSet rs = stmt.executeQuery();
            List<String> bvs = new ArrayList<>();
            while (rs.next()) {
                bvs.add(rs.getString(1));
            }

            long maxAv = 0;
            for (int i = 0; i < bvs.size(); i++) {
                String bv = bvs.get(i);
                long av = bv2av(bv);
                maxAv = Math.max(maxAv, av);
                //System.out.println(av);
            }
            maxAv++;
            String bv = av2bv(maxAv);

            rs.close();
            stmt.close();
            return bv;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static final String key = "fZodR9XQDSUm21yCkr6zBqiveYah8bt4xsWpHnJE7jL5VG3guMTKNPAwcF";
    private static final int[] pos = {11, 10, 3, 8, 4, 6};
    private static final long xorNum = 177451812;
    private static final long minusNum = 8728348608L;

    private static long bv2av(String bv) {
        long num = 0;
        for (int i = 0; i < pos.length; i++)
            num = (long) (num + key.indexOf(bv.charAt(pos[i])) * Math.pow(58, i));
        return (num - minusNum) ^ xorNum;
    }

    private static String av2bv(long av) {
        av = (av ^ xorNum) + minusNum;
        String[] tmp = "BV1  4 1 7  ".split("");
        for (int i = 0; i < pos.length; i++)
            tmp[pos[i]] = key.split("")[(int) (av / Math.pow(58, i) % 58)];
        return String.join("", tmp);
    }

}
