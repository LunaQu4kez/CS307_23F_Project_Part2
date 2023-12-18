package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
@Service
@Slf4j
public class DanmuServiceImpl implements DanmuService {
    @Autowired
    private DataSource dataSource;
    @Override

    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {
        if (!Authentication.authentication(auth, dataSource)){
            return -1;
        }
        try (Connection conn = dataSource.getConnection()) {
            String sql1 = "select bv, can_see, public_time from video_info where bv = ?";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()){
                rs.close();
                stmt.close();
                return -1;
            }
            Timestamp current = new Timestamp(System.currentTimeMillis());
            if (content == null || content.equals("") || current.before(rs.getTimestamp(3))){
                rs.close();
                stmt.close();
                return -1;
            }

            if (!rs.getBoolean(2)){
                rs.close();
                stmt.close();
                return -1;
            }

            String sql2 = "select * from view_video where mid = ? and bv = ?";
            stmt = conn.prepareStatement(sql2);
            stmt.setLong(1, auth.getMid());
            stmt.setString(2, bv);
            rs = stmt.executeQuery();
            if (!rs.next()){
                rs.close();
                stmt.close();
                return -1;
            }
            rs.close();
            stmt.close();

            String sql4 = "select count(danmu_id) from danmu_info";
            stmt = conn.prepareStatement(sql4);
            rs = stmt.executeQuery();
            rs.next();
            int cnt = rs.getInt(1);

            String sql3 = "insert into danmu_info (danmu_id, bv, mid, time, content, post_time) " + "values (?,?,?,?,?,?)";
            PreparedStatement stmt1 = conn.prepareStatement(sql3, Statement.RETURN_GENERATED_KEYS);
            stmt1.setLong(1, cnt + 1);
            stmt1.setString(2, bv);
            stmt1.setLong(3, auth.getMid());
            stmt1.setFloat(4, time);
            stmt1.setString(5, content);
            stmt1.setTimestamp(6, current);
            int result = stmt1.executeUpdate();
            stmt1.close();
            return result > 0 ? (cnt + 1) : -1;
        }catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {
        try (Connection conn = dataSource.getConnection()) {
            String sql1 = "select * from video_info where bv = ?";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if(!rs.next()){
                rs.close();
                stmt.close();
                return null;
            }

            if (timeStart >= timeEnd || timeStart < 0 || timeEnd > rs.getFloat(3)){
                rs.close();
                stmt.close();
                return null;
            }

            Timestamp current = new Timestamp(System.currentTimeMillis());
            if (!rs.getBoolean(10) || current.before(rs.getTimestamp(9))){
                rs.close();
                stmt.close();
                return null;
            }

            List<Long> danmu = new ArrayList<>();
            if (!filter){
                String sql2 = "select * from danmu_info where bv = ? and time between ? and ?";
                stmt = conn.prepareStatement(sql2);
                stmt.setString(1, bv);
                stmt.setFloat(2, timeStart);
                stmt.setFloat(3, timeEnd);
                rs = stmt.executeQuery();
                while (rs.next()){
                    long id = rs.getLong(1);
                    danmu.add(id);
                }
                rs.close();
                stmt.close();
                return danmu;
            }else {
                String sql3 = "select danmu_id from danmu_info d join (\n" +
                        "select content, min(time) as time from danmu_info\n" +
                        "where bv = ? and time between ? and ?\n" +
                        "group by content) s on d.content = s.content and d.time = s.time\n" +
                        "order by d.time";
                stmt = conn.prepareStatement(sql3);
                stmt.setString(1, bv);
                stmt.setFloat(2, timeStart);
                stmt.setFloat(3, timeEnd);
                rs = stmt.executeQuery();
                while (rs.next()){
                    long id = rs.getLong(1);
                    danmu.add(id);
                }
                rs.close();
                stmt.close();
                return danmu;
            }
        }catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {
        if (!Authentication.authentication(auth, dataSource)){
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            String sql1 = "select danmu_id, bv from danmu_info where danmu_id = ?";
            PreparedStatement stmt = conn.prepareStatement(sql1);
            stmt.setLong(1, id);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()){
                rs.close();
                stmt.close();
                return false;
            }

            String bv = rs.getString(2);
            String sql5 = "select * from view_video where bv= ? and mid = ?";
            stmt = conn.prepareStatement(sql5);
            stmt.setString(1, bv);
            stmt.setLong(2, auth.getMid());
            rs = stmt.executeQuery();
            if (!rs.next()){
                rs.close();
                stmt.close();
                return false;
            }

            String sql2 = "select * from like_danmu where mid = ? and danmu_id = ?";
            stmt = conn.prepareStatement(sql2);
            stmt.setLong(1, auth.getMid());
            stmt.setLong(2, id);
            rs = stmt.executeQuery();
            if (!rs.next()){
                String sql3 = "insert into like_danmu (mid, danmu_id) values (?,?)";
                stmt = conn.prepareStatement(sql3);
                stmt.setLong(1, auth.getMid());
                stmt.setLong(2, id);
                stmt.executeUpdate();
                rs.close();
                stmt.close();
                return true;
            }else {
                String sql4 = "delete from like_danmu where mid = ? and danmu_id = ?";
                stmt = conn.prepareStatement(sql4);
                stmt.setLong(1, auth.getMid());
                stmt.setLong(2, id);
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
}
