package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {
    public static int BATCH_SIZE = 1000;

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
        return Arrays.asList(12211655, 12211308, 12110120);
    }

    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String sql1 = "insert into user_info values(?, ?, ?, ?, ?, ?, ?, ?);";
        String sql2 = "insert into user_auth values(?, ?, ?, ?)";
        String sql3 = "insert into max_mid values(?)";
        try (PreparedStatement stmt1 = conn.prepareStatement(sql1);
             PreparedStatement stmt2 = conn.prepareStatement(sql2);
             PreparedStatement stmt3 = conn.prepareStatement(sql3);) {
            long cnt1 = 0;
            long maxId = 0;
            for (UserRecord user : userRecords) {
                maxId = Math.max(maxId, user.getMid());
                stmt1.setLong(1, user.getMid());
                stmt1.setString(2, user.getName());
                stmt1.setString(3, user.getSex());
                stmt1.setString(4, user.getBirthday());
                stmt1.setInt(5, user.getLevel());
                stmt1.setInt(6, user.getCoin());
                stmt1.setString(7, user.getSign());
                if (user.getIdentity() == UserRecord.Identity.USER) {
                    stmt1.setString(8, "user");
                } else {
                    stmt1.setString(8, "superuser");
                }
                stmt1.addBatch();
                stmt2.setLong(1, user.getMid());
                stmt2.setString(2, user.getPassword());
                stmt2.setString(3, user.getQq());
                stmt2.setString(4, user.getWechat());
                stmt2.addBatch();
                cnt1++;
                if (cnt1 % BATCH_SIZE == 0) {
                    stmt1.executeBatch();
                    stmt1.clearBatch();
                    stmt2.executeBatch();
                    stmt2.clearBatch();
                }
            }
            if (cnt1 % BATCH_SIZE != 0) {
                stmt1.executeBatch();
                stmt1.clearBatch();
                stmt2.executeBatch();
                stmt2.clearBatch();
            }
            if (maxId != 0) {
                stmt3.setLong(1, maxId);
                stmt3.addBatch();
                stmt3.executeBatch();
                stmt3.clearBatch();
                stmt3.close();
            }
            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
                e.printStackTrace();
            } catch (SQLException e2) {
                e2.printStackTrace();
            }
        }
        String sql4 = "insert into follow values(?,?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql4)) {
            long cnt = 0;
            for (UserRecord user : userRecords) {
                long[] follow = user.getFollowing();
                for (int i = 0; i < follow.length; i++) {
                    stmt.setLong(1, user.getMid());
                    stmt.setLong(2, follow[i]);
                    stmt.addBatch();
                    cnt++;
                    if (cnt % BATCH_SIZE == 0) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                    }
                }
                if (cnt % BATCH_SIZE == 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt.executeBatch();
                stmt.clearBatch();
            }
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e2) {
                e2.printStackTrace();
            }
        }


        long maxBv = 0;
        String sql5 = "insert into video_info values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        String sql6 = "insert into max_bv values(?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql5);
             PreparedStatement stmt2 = conn.prepareStatement(sql6)) {
            long cnt = 0;
            for (VideoRecord video : videoRecords) {
                maxBv = Math.max(maxBv, Long.parseLong(video.getBv().substring(2)));
                stmt.setString(1, video.getBv());
                stmt.setString(2, video.getTitle());
                stmt.setFloat(3, video.getDuration());
                stmt.setString(4, video.getDescription());
                stmt.setLong(5, video.getOwnerMid());
                stmt.setLong(6, video.getReviewer());
                stmt.setTimestamp(7, video.getCommitTime());
                stmt.setTimestamp(8, video.getReviewTime());
                stmt.setTimestamp(9, video.getPublicTime());
                stmt.setInt(10, video.getLike().length);
                stmt.setInt(11, video.getCoin().length);
                stmt.setInt(12, video.getFavorite().length);
                stmt.setInt(13, 0);
                stmt.setInt(14, 0);
                stmt.setBoolean(15, video.getReviewer() != null);
                stmt.addBatch();
                cnt++;
                if (cnt % BATCH_SIZE == 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt.executeBatch();
                stmt.clearBatch();
            }
            if (maxBv != 0) {
                stmt2.setString(1, "BV" + maxBv);
                stmt2.setLong(2, maxBv);
                stmt2.addBatch();
                stmt2.executeBatch();
                stmt2.clearBatch();
                stmt2.close();
            }
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }

        String sql7 = "insert into like_video values(?,?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql7)) {
            long cnt = 0;
            for (VideoRecord video : videoRecords) {
                long[] follow = video.getLike();
                String bv = video.getBv();
                for (int i = 0; i < follow.length; i++) {
                    stmt.setLong(1, follow[i]);
                    stmt.setString(2, bv);
                    stmt.addBatch();
                    cnt++;
                    if (cnt % BATCH_SIZE == 0) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                    }
                }
                if (cnt % BATCH_SIZE == 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt.executeBatch();
                stmt.clearBatch();
            }
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e2) {
                e2.printStackTrace();
            }
        }

        String sql8 = "insert into coin_video values(?,?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql8)) {
            long cnt = 0;
            for (VideoRecord video : videoRecords) {
                long[] follow = video.getCoin();
                String bv = video.getBv();
                for (int i = 0; i < follow.length; i++) {
                    stmt.setLong(1, follow[i]);
                    stmt.setString(2, bv);
                    stmt.addBatch();
                    cnt++;
                    if (cnt % BATCH_SIZE == 0) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                    }
                }
                if (cnt % BATCH_SIZE == 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt.executeBatch();
                stmt.clearBatch();
            }
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e2) {
                e2.printStackTrace();
            }
        }
        String sql9 = "insert into fav_video values(?,?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql9)) {
            long cnt = 0;
            for (VideoRecord video : videoRecords) {
                long[] follow = video.getFavorite();
                String bv = video.getBv();
                for (int i = 0; i < follow.length; i++) {
                    stmt.setLong(1, follow[i]);
                    stmt.setString(2, bv);
                    stmt.addBatch();
                    cnt++;
                    if (cnt % BATCH_SIZE == 0) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                    }
                }
                if (cnt % BATCH_SIZE == 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt.executeBatch();
                stmt.clearBatch();
            }
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e2) {
                e2.printStackTrace();
            }
        }

        String sql10 = "insert into view_video values(?,?,?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql10)) {
            long cnt = 0;
            for (VideoRecord video : videoRecords) {
                long[] view = video.getViewerMids();
                float[] viewTime = video.getViewTime();
                String bv = video.getBv();
                for (int i = 0; i < view.length; i++) {
                    stmt.setLong(1, view[i]);
                    stmt.setString(2, bv);
                    stmt.setFloat(3, viewTime[i]);
                    stmt.addBatch();
                    cnt++;
                    if (cnt % BATCH_SIZE == 0) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                    }
                }
                if (cnt % BATCH_SIZE == 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt.executeBatch();
                stmt.clearBatch();
            }
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e2) {
                e2.printStackTrace();
            }
        }

        String sql11 = "insert into danmu_info (bv , mid , time, content , post_time) values ( ?, ?, ?, ? ,? )";
        try(PreparedStatement stmt = conn.prepareStatement(sql11)){
            long cnt = 0;
            for (int i = 0; i < danmuRecords.size(); i++) {
                DanmuRecord danmu =danmuRecords.get(i);
                stmt.setString(1,danmu.getBv());
                stmt.setLong(2,danmu.getMid());
                stmt.setFloat(3,danmu.getTime());
                stmt.setString(4,danmu.getContent());
                stmt.setTimestamp(5,danmu.getPostTime());
                cnt++;
                stmt.addBatch();
                if (cnt % BATCH_SIZE == 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt.executeBatch();
                stmt.clearBatch();
            }
            conn.commit();
        }catch (Exception e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e2) {
                e2.printStackTrace();
            }
        }


        String sql12 = "insert into like_danmu values(?,?)";
        try(PreparedStatement stmt = conn.prepareStatement(sql12) ) {
            long cnt = 0;
            for (int i = 0; i < danmuRecords.size(); i++) {
                DanmuRecord danmu = danmuRecords.get(i);
                long[] follow = danmu.getLikedBy();
                for (int j = 0; j < follow.length; j++) {
                    stmt.setLong(1, follow[j]);
                    stmt.setInt(2, i+1);
                    stmt.addBatch();
                    cnt++;
                    if (cnt % BATCH_SIZE == 0) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                    }
                }
                if (cnt % BATCH_SIZE == 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt.executeBatch();
                stmt.clearBatch();
            }
            conn.commit();
        }catch (Exception e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e2) {
                e2.printStackTrace();
            }
        }
        try {
            conn.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.
        String[] tableName = {"like_danmu", "view_video", "like_video", "fav_video", "coin_video", "max_bv",
                "danmu_info", "video_info", "follow", "max_mid", "user_auth", "user_info"};
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            for (int i = 0; i < tableName.length; i++) {
                String sql = "truncate table " + tableName[i] + " cascade";
                stmt.execute(sql);
            }
        } catch (SQLException e) {

            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
