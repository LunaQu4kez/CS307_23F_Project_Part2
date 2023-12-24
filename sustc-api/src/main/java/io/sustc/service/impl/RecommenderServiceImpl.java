package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.RecommenderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class RecommenderServiceImpl implements RecommenderService {
    @Autowired
    private DataSource dataSource;

    @Override
    public List<String> recommendNextVideo(String bv) {
        String sql1 = "select * from video_info where bv = ?";
        String sql2 = "WITH video_viewers AS (SELECT mid, bv FROM view_video WHERE bv = ?)\n" +
                "SELECT v1.bv, COUNT(*) AS num_common_viewers\n" +
                "FROM view_video v1 JOIN video_info v2 on v1.bv = v2.bv\n" +
                "WHERE v1.mid IN (SELECT video_viewers.mid FROM video_viewers)\n" +
                "  AND v2.bv <> ?\n" +
                "  AND v2.can_see IS TRUE\n" +
                "  AND ? >= v2.public_time\n" +
                "GROUP BY v1.bv\n" +
                "ORDER BY num_common_viewers DESC\n" +
                "LIMIT 5";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt1 = conn.prepareStatement(sql1)) {
            stmt1.setString(1, bv);
            ResultSet rs = stmt1.executeQuery();
            if (!rs.next()) {
                return null;
            }
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql2)) {
            stmt.setString(1, bv);
            stmt.setString(2, bv);
            stmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            List<String> result = new ArrayList<>();
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                result.add(rs.getString("bv"));
            }
            rs.close();
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {
        if (pageSize < 1 || pageNum < 1) return null;
        String sql = "select view_cnt.bv,\n" +
                "       count_view,\n" +
                "       count_like,\n" +
                "       count_coin,\n" +
                "       count_fav,\n" +
                "       count_danmu,\n" +
                "       count_fin,\n" +
                "       case\n" +
                "           when count_view = 0 then 0\n" +
                "           else\n" +
                "                   case\n" +
                "                       when cast(count_like as float) / cast(count_view as float) > 1 then 1\n" +
                "                       else cast(count_like as float) / cast(count_view as float) end +\n" +
                "                   case\n" +
                "                       when cast(count_coin as float) / cast(count_view as float) > 1 then 1\n" +
                "                       else cast(count_coin as float) / cast(count_view as float) end +\n" +
                "                   case\n" +
                "                       when cast(count_fav as float) / cast(count_view as float) > 1 then 1\n" +
                "                       else cast(count_fav as float) / cast(count_view as float) end +\n" +
                "                   case\n" +
                "                       when cast(count_danmu as float) / cast(count_view as float) > 1 then 1\n" +
                "                       else cast(count_danmu as float) / cast(count_view as float) end +\n" +
                "                   case\n" +
                "                       when cast(count_fin as float) / (cast(count_view as float) *cast(duration as float))  > 1 then 1\n" +
                "                       else cast(count_fin as float) / (cast(count_view as float) *cast(duration as float)) end end as rate\n" +
                "from (select bv, count(*)as count_view  from view_video group by bv) view_cnt\n" +
                "         join(select bv, count(*) as count_like\n" +
                "              from like_video\n" +
                "              group by bv) like_cnt on view_cnt.bv = like_cnt.bv\n" +
                "         join (select bv, count(*) as count_coin\n" +
                "               from coin_video\n" +
                "               group by bv) coin_cnt on view_cnt.bv = coin_cnt.bv\n" +
                "         join (select bv, count(*) as count_fav\n" +
                "               from fav_video\n" +
                "               group by bv) fav_cnt on view_cnt.bv = fav_cnt.bv\n" +
                "         join (select bv, count(*) as count_danmu\n" +
                "               from danmu_info\n" +
                "               group by bv) dannmu_cnt on view_cnt.bv = dannmu_cnt.bv\n" +
                "         join (select vv.bv, vi.duration as duration ,sum(vv.time) as count_fin\n" +
                "               from view_video vv join video_info vi on vv.bv = vi.bv\n" +
                "               group by vv.bv ,vi.bv) fin_cnt on view_cnt.bv = fin_cnt.bv\n" +
                "order by rate desc;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            List<String> result = new ArrayList<>();
            ResultSet rs = stmt.executeQuery();
            for (int i = 1; i <= pageNum * pageSize; i++)
                if (rs.next() && i > pageSize * (pageNum - 1)) result.add(rs.getString("bv"));
            rs.close();
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {
        long auth_mid = Authentication.authentication(auth, dataSource);
        if (pageSize <= 0 || pageNum <= 0 || auth_mid == 0) return null;
        String sql1 = "select up_mid,fans_mid from follow where up_mid = ? intersect select up_mid,fans_mid from follow where fans_mid = ?";
        String sql2 = "select v.bv, count(*) as cnt\n" +
                "from view_video v\n" +
                "         join (select up_mid, fans_mid as friends_mid\n" +
                "               from follow\n" +
                "               where (fans_mid, up_mid) in (select up_mid, fans_mid from follow)\n" +
                "                 and up_mid = ?) f on f.friends_mid = v.mid\n" +
                "         join video_info i on v.bv = i.bv\n" +
                "         join user_info ui on i.owner_mid = ui.mid\n" +
                "where (v.bv, f.up_mid) not in (select bv, mid from view_video where mid = ?) and i.can_see = true and ? >= i.public_time\n" +
                "group by v.bv, ui.level, i.public_time\n" +
                "order by cnt desc, ui.level, i.public_time;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql1)) {
            stmt.setLong(1, auth_mid);
            stmt.setLong(2, auth_mid);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                return generalRecommendations(pageSize, pageNum);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql2)) {
            List<String> result = new ArrayList<>();
            stmt.setLong(1, auth_mid);
            stmt.setLong(2, auth_mid);
            stmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            ResultSet rs = stmt.executeQuery();
            for (int i = 1; i <= pageNum * pageSize; i++){
                if (rs.next() && i > pageSize * (pageNum - 1)) result.add(rs.getString("bv"));
            }
            rs.close();
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        long auth_mid = Authentication.authentication(auth, dataSource);
        if (pageSize <= 0 || pageNum <= 0 || auth_mid == 0) return null;
        String sql = "select count(*) as cnt, af.up_mid as rec " +
                "from (select up_mid, fans_mid from follow where fans_mid = ?) af " +
                "join (select up_mid, fans_mid from follow where fans_mid <> ?) bf " +
                "on af.up_mid = bf.up_mid " +
                "join user_info on af.up_mid = user_info.mid " +
                "where (bf.fans_mid, af.fans_mid) not in (select up_mid, fans_mid from follow where fans_mid = ?) " +
                "group by af.up_mid, level order by cnt desc, level desc, rec asc";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            List<Long> result = new ArrayList<>();
            stmt.setLong(1, auth_mid);
            stmt.setLong(2, auth_mid);
            stmt.setLong(3, auth_mid);
            ResultSet rs = stmt.executeQuery();
            for (int i = 1; i <= pageNum * pageSize; i++)
                if (rs.next() && i > pageSize * (pageNum - 1)) result.add(rs.getLong("rec"));
            rs.close();
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
