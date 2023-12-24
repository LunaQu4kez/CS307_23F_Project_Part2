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
import java.util.*;

@Service
@Slf4j
public class VideoServiceImpl implements VideoService {

    @Autowired
    private DataSource dataSource;

    // accept
    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req) {
        long auth_mid = Authentication.authentication(auth, dataSource);
        if (auth_mid == 0) {
            return null;
        }

        try (Connection conn = dataSource.getConnection()) {
            Timestamp ts = new Timestamp(System.currentTimeMillis());
            if (req.getTitle() == null || req.getTitle().equals("") || req.getDuration() < 10
                    || req.getPublicTime() == null || ts.after(req.getPublicTime())) {
                return null;
            }
            String sql1 = "select * from video_info where title = ? and owner_mid = ?";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            stmt.setString(1, req.getTitle());
            stmt.setLong(2, auth_mid);
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
            stmt.setLong(5, auth_mid);
            stmt.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
            stmt.setTimestamp(7, req.getPublicTime());
            stmt.executeUpdate();

            rs.close();
            stmt.close();
            return bv;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("+++++++++++++++++++++++++");
            return null;
        }
    }

    // accept
    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {
        long auth_mid = Authentication.authentication(auth, dataSource);
        if (auth_mid == 0) {
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
            stmt.setLong(1, auth_mid);
            rs = stmt.executeQuery();
            rs.next();
            String identity = rs.getString(1);
            if (!identity.equals("superuser") && owner_mid != auth_mid) {
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

    // accept
    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        long auth_mid = Authentication.authentication(auth, dataSource);
        if (auth_mid == 0) {
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            String sql1 = "select owner_mid, title, description, duration, public_time from video_info where bv = ?";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                rs.close();
                stmt.close();
                return false;
            }
            long ownerMid = rs.getLong(1);
            String title = rs.getString(2);
            String description = rs.getString(3);
            double duration = rs.getDouble(4);
            Timestamp publicTime = rs.getTimestamp(5);
            if (ownerMid != auth_mid) {
                rs.close();
                stmt.close();
                return false;
            }

            Timestamp now = new Timestamp(System.currentTimeMillis());
            if (req.getTitle() == null || req.getTitle().equals("") || req.getDuration() < 10
                    || req.getPublicTime() == null || now.after(req.getPublicTime())) {
                rs.close();
                stmt.close();
                return false;
            }

            if ((duration != req.getDuration()) || (title.equals(req.getTitle()) &&
                    description.equals(req.getDescription()) && duration == req.getDuration()
                    && publicTime.equals(req.getPublicTime()))) {
                rs.close();
                stmt.close();
                return false;
            }

            String sql2 = "update video_info set title = ?, description = ?, public_time = ?, " +
                    "can_see = false where bv = ?";
            stmt = conn.prepareStatement(sql2);
            stmt.setString(1, req.getTitle());
            stmt.setString(2, req.getDescription());
            if (!now.after(req.getPublicTime()))
                stmt.setTimestamp(3, req.getPublicTime());
            stmt.setString(4, bv);
            int x = stmt.executeUpdate();
            stmt.close();
            return x != 0;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    // toDo: debug
    @Override
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        long auth_mid = Authentication.authentication(auth, dataSource);
        if (auth_mid == 0){
            return null;
        }
        if (keywords == null || keywords.equals("") || pageSize <= 0 || pageNum <= 0) {
            return null;
        }
        try (Connection conn = dataSource.getConnection()) {
            List<String> bvs = new ArrayList<>();
            Map<String, Integer> match = new HashMap<>();
            Map<String, Integer> watch = new HashMap<>();
            Timestamp now = new Timestamp(System.currentTimeMillis());

            String sql0 = "select identity from user_info where mid = ?";
            PreparedStatement stmt = conn.prepareStatement(sql0);
            stmt.setLong(1, auth_mid);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            String identity = rs.getString(1);

            String sql1 = "select bv, title, description, name, can_see, public_time " +
                    "from video_info join user_info on mid = owner_mid";
            stmt = conn.prepareStatement(sql1);
            rs = stmt.executeQuery();
            while (rs.next()) {
                String bv = rs.getString(1);
                String title = rs.getString(2);
                String description = rs.getString(3);
                String name = rs.getString(4);
                boolean canSee = rs.getBoolean(5);
                Timestamp publicTime = rs.getTimestamp(6);
                if (identity.equals("user") && (!canSee || now.before(publicTime))) {
                    continue;
                }
                String[] words = keywords.split(" ");
                for (int i = 0; i < words.length; i++) {
                    if (!words.equals("")) {
                        String[] word = (words[i] + "!").split("%");
                        words[i] = word[0];
                        for (int j = 1; j < word.length; j++) {
                            words[i] = words[i] + "\\%" + word[j];
                        }
                        words[i] = words[i].substring(0, words[i].length() - 1);
                    }
                }
                int cnt = 0;
                for (int i = 0; i < words.length; i++) {
                    if (words[i].length() > 0) {
                        cnt += cntStr(title.toLowerCase(), words[i].toLowerCase());
                        cnt += cntStr(description.toLowerCase(), words[i].toLowerCase());
                        cnt += cntStr(name.toLowerCase(), words[i].toLowerCase());
                    }
                }
                if (cnt > 0) {
                    match.put(bv, cnt);
                    bvs.add(bv);
                }
            }

            String sql2 = "select bv, count(mid) from view_video group by bv";
            stmt = conn.prepareStatement(sql2);
            rs = stmt.executeQuery();
            while (rs.next()) {
                String bv = rs.getString(1);
                int cnt = rs.getInt(2);
                watch.put(bv, cnt);
            }

            rs.close();
            stmt.close();

            bvs.sort(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    if (!Objects.equals(match.get(o1), match.get(o2)))
                        return -match.get(o1) + match.get(o2);
                    else return -watch.getOrDefault(o1, 0) + watch.getOrDefault(o2, 0);
                }
            });

            List<String> ans = new ArrayList<>();
            for (int i = (pageNum - 1) * pageSize; i < Math.min(pageNum * pageSize, bvs.size()); i++) {
                ans.add(bvs.get(i));
            }
            return ans;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // accept
    @Override
    public double getAverageViewRate(String bv) {
        try (Connection conn = dataSource.getConnection()) {
            String sql1 = "select duration from video_info where bv = ?";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                rs.close();
                stmt.close();
                return -1;
            }
            double duration = rs.getDouble(1);

            String sql2 = "select count(*) from view_video where bv = ?";
            stmt = conn.prepareStatement(sql2);
            stmt.setString(1, bv);
            rs = stmt.executeQuery();
            rs.next();
            int cnt = rs.getInt(1);
            if (cnt == 0) {
                rs.close();
                stmt.close();
                return -1;
            }

            String sql3 = "select time from view_video where bv = ?";
            stmt = conn.prepareStatement(sql3);
            stmt.setString(1, bv);
            rs = stmt.executeQuery();
            double totalViewRate = 0;
            while (rs.next()) {
                totalViewRate += rs.getDouble(1) / duration;
            }
            rs.close();
            stmt.close();
            return totalViewRate / cnt;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    // accept
    @Override
    public Set<Integer> getHotspot(String bv) {
        Set<Integer> set = new HashSet<>();
        try (Connection conn = dataSource.getConnection()) {
            String sql1 = "select duration from video_info where bv = ?";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                rs.close();
                stmt.close();
                return set;
            }
            double duration = rs.getDouble(1);
            //System.out.println("duration = " + duration);

            String sql2 = "select count(*) from danmu_info where bv = ?";
            stmt = conn.prepareStatement(sql2);
            stmt.setString(1, bv);
            rs = stmt.executeQuery();
            rs.next();
            if (rs.getInt(1) == 0) {
                rs.close();
                stmt.close();
                return set;
            }

            int[] cnt = new int[(int) (duration / 10) + 1];
            int maxCnt = 0;
            String sql3 = "select time from danmu_info where bv = ?";
            stmt = conn.prepareStatement(sql3);
            stmt.setString(1, bv);
            rs = stmt.executeQuery();
            while (rs.next()) {
                double time = rs.getDouble(1);
                cnt[(int) (time / 10)]++;
                maxCnt = Math.max(maxCnt, cnt[(int) (time / 10)]);
            }
            for (int i = 0; i < cnt.length; i++) {
                //System.out.println("cnt[" + i + "] = " + cnt[i]);
                if (cnt[i] == maxCnt) set.add(i);
            }
            return set;
        } catch (Exception e) {
            e.printStackTrace();
            return set;
        }
    }

    // accept
    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        long auth_mid = Authentication.authentication(auth, dataSource);
        if (auth_mid == 0) {
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            String sql1 = "select owner_mid, can_see from video_info where bv = ?";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                rs.close();
                stmt.close();
                return false;
            }
            long ownerMid = rs.getLong(1);
            boolean canSee = rs.getBoolean(2);


            String sql2 = "select identity from user_info where mid = ?";
            stmt = conn.prepareStatement(sql2);
            stmt.setLong(1, auth_mid);
            rs = stmt.executeQuery();
            rs.next();
            String identity = rs.getString(1);
            if (!identity.equals("superuser")) {
                rs.close();
                stmt.close();
                return false;
            }

            if (ownerMid == auth_mid || canSee) {
                rs.close();
                stmt.close();
                if (bv.equals("BV19t4y1E7hd"))
                    System.out.println("3333333333333333333333333333");
                return false;
            }

            String sql3 = "update video_info set reviewer_mid = ?, review_time = ?, can_see = true" +
                    " where bv = ?";
            stmt = conn.prepareStatement(sql3);
            stmt.setLong(1, auth_mid);
            stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            stmt.setString(3, bv);
            int x = stmt.executeUpdate();
            stmt.close();
            return x != 0;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    // accept
    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        long auth_mid = Authentication.authentication(auth, dataSource);
        if (auth_mid == 0) {
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
            stmt.setLong(1, auth_mid);
            rs = stmt.executeQuery();
            rs.next();
            String identity = rs.getString(1);
            int coin = rs.getInt(2);

            if (auth_mid == ownerMid) {
                rs.close();
                stmt.close();
                return false;
            }
            if ((identity.equals("user") && canSee && new Timestamp(System.currentTimeMillis()).after(publicTime)) ||
                    (identity.equals("superuser"))) {
                String sql3 = "select * from coin_video where mid = ? and bv = ?";
                stmt = conn.prepareStatement(sql3);
                stmt.setLong(1, auth_mid);
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
                stmt.setLong(2, auth_mid);
                int x = stmt.executeUpdate();
                if (x == 0) {
                    rs.close();
                    stmt.close();
                    return false;
                }

                String sql5 = "update user_info set coin = ? where mid = ?";
                stmt = conn.prepareStatement(sql5);
                stmt.setInt(1, coin - 1);
                stmt.setLong(2, auth_mid);
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

    // accept
    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        //boolean b = auth.getMid() == 596082;
        long auth_mid = Authentication.authentication(auth, dataSource);
        if (auth_mid == 0) {
            //System.out.println("111111111111111111111111111111");
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
                //System.out.println("222222222222222222222222222");
                return false;
            }
            long ownerMid = rs.getLong(1);
            Timestamp publicTime = rs.getTimestamp(2);
            boolean canSee = rs.getBoolean(3);

            String sql2 = "select identity from user_info where mid = ?";
            stmt = conn.prepareStatement(sql2);
            stmt.setLong(1, auth_mid);
            rs = stmt.executeQuery();
            rs.next();
            String identity = rs.getString(1);

//            if (auth_mid == ownerMid) {
//                rs.close();
//                stmt.close();
//                System.out.println("333333333333333333333333333333");
//                return false;
//            }
            if ((identity.equals("user") && canSee && new Timestamp(System.currentTimeMillis()).after(publicTime)) ||
                    (identity.equals("superuser"))) {
                String sql3 = "select * from like_video where mid = ? and bv = ?";
                stmt = conn.prepareStatement(sql3);
                stmt.setLong(1, auth_mid);
                stmt.setString(2, bv);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    String sql4 = "delete from like_video where bv = ? and mid = ?";
                    stmt = conn.prepareStatement(sql4);
                    stmt.setString(1, bv);
                    stmt.setLong(2, auth_mid);
                    //System.out.println("----------------------");
                    return stmt.executeUpdate() == 0;
                } else {
                    String sql4 = "insert into like_video (bv, mid) values (?, ?)";
                    stmt = conn.prepareStatement(sql4);
                    stmt.setString(1, bv);
                    stmt.setLong(2, auth_mid);
                    //System.out.println("+++++++++++++++++++++++");
                    return stmt.executeUpdate() != 0;
                }
            } else {
                rs.close();
                stmt.close();
                //System.out.println("555555555555555555555555555555555555");
                return false;
            }

        } catch (Exception e) {
            e.printStackTrace();
            //System.out.println("6666666666666666666666666666666666666666");
            return false;
        }
    }

    // accept
    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        long auth_mid = Authentication.authentication(auth, dataSource);
        if (auth_mid == 0) {
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
            stmt.setLong(1, auth_mid);
            rs = stmt.executeQuery();
            rs.next();
            String identity = rs.getString(1);

//            if (auth_mid == ownerMid) {
//                rs.close();
//                stmt.close();
//                return false;
//            }
            if ((identity.equals("user") && canSee && new Timestamp(System.currentTimeMillis()).after(publicTime)) ||
                    (identity.equals("superuser"))) {
                String sql3 = "select * from fav_video where mid = ? and bv = ?";
                stmt = conn.prepareStatement(sql3);
                stmt.setLong(1, auth_mid);
                stmt.setString(2, bv);
                rs = stmt.executeQuery();
                if (rs.next()) {
                    String sql4 = "delete from fav_video where bv = ? and mid = ?";
                    stmt = conn.prepareStatement(sql4);
                    stmt.setString(1, bv);
                    stmt.setLong(2, auth_mid);
                    return stmt.executeUpdate() == 0;
                } else {
                    String sql4 = "insert into fav_video (bv, mid) values (?, ?)";
                    stmt = conn.prepareStatement(sql4);
                    stmt.setString(1, bv);
                    stmt.setLong(2, auth_mid);
                    return stmt.executeUpdate() != 0;
                }
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

    private static int cntStr(String str, String mode) {
        int cnt = 0;
        for (int i = 0; i < str.length() - mode.length() + 1; i++) {
            if (mode.equals(str.substring(i, i + mode.length()))) {
                cnt++;
            }
        }
        return cnt;
    }
}
