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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {
    public static int BATCH_SIZE = 1000;
    public static boolean USE_FASTER_IMPORT = true;

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
        if (!USE_FASTER_IMPORT) {
            importData0(danmuRecords, userRecords, videoRecords);
            return;
        }
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
                stmt2.setString(2, Authentication.hash(user.getPassword(),user.getMid()));
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

        insertInFollow(userRecords);

        String sql5 = "insert into video_info values(?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql5)) {
            long cnt = 0;
            for (VideoRecord video : videoRecords) {
                stmt.setString(1, video.getBv());
                stmt.setString(2, video.getTitle());
                stmt.setFloat(3, video.getDuration());
                stmt.setString(4, video.getDescription());
                stmt.setLong(5, video.getOwnerMid());
                stmt.setLong(6, video.getReviewer());
                stmt.setTimestamp(7, video.getCommitTime());
                stmt.setTimestamp(8, video.getReviewTime());
                stmt.setTimestamp(9, video.getPublicTime());
                stmt.setBoolean(10, video.getReviewer() != null);
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
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }

        insert("like_video", videoRecords);

        insert("coin_video", videoRecords);

        insert("fav_video", videoRecords);

        insert(videoRecords);

        String sql11 = "insert into danmu_info (danmu_id , bv , mid , time, content , post_time) values ( ? , ?, ?, ?, ? ,? )";
        try (PreparedStatement stmt = conn.prepareStatement(sql11)) {
            long cnt = 0;
            for (int i = 0; i < danmuRecords.size(); i++) {
                DanmuRecord danmu = danmuRecords.get(i);
                stmt.setLong(1, cnt + 1);
                stmt.setString(2, danmu.getBv());
                stmt.setLong(3, danmu.getMid());
                stmt.setFloat(4, danmu.getTime());
                stmt.setString(5, danmu.getContent());
                stmt.setTimestamp(6, danmu.getPostTime());
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
        } catch (Exception e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e2) {
                e2.printStackTrace();
            }
        }
        insertInLikeDanmu(danmuRecords);
    }

    public void importData0(
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
                    stmt.setLong(2, user.getMid());
                    stmt.setLong(1, follow[i]);
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


        String sql5 = "insert into video_info values(?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql5)) {
            long cnt = 0;
            for (VideoRecord video : videoRecords) {
                stmt.setString(1, video.getBv());
                stmt.setString(2, video.getTitle());
                stmt.setFloat(3, video.getDuration());
                stmt.setString(4, video.getDescription());
                stmt.setLong(5, video.getOwnerMid());
                stmt.setLong(6, video.getReviewer());
                stmt.setTimestamp(7, video.getCommitTime());
                stmt.setTimestamp(8, video.getReviewTime());
                stmt.setTimestamp(9, video.getPublicTime());
                stmt.setBoolean(10, video.getReviewer() != null);
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
                long[] viewer = video.getViewerMids();
                float[] time = video.getViewTime();
                String bv = video.getBv();
                for (int i = 0; i < viewer.length; i++) {
                    stmt.setLong(1, viewer[i]);
                    stmt.setString(2, bv);
                    stmt.setFloat(3, time[i]);
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

        String sql11 = "insert into danmu_info (danmu_id , bv , mid , time, content , post_time) values ( ? , ?, ?, ?, ? ,? )";
        try (PreparedStatement stmt = conn.prepareStatement(sql11)) {
            long cnt = 0;
            for (int i = 0; i < danmuRecords.size(); i++) {
                DanmuRecord danmu = danmuRecords.get(i);
                stmt.setLong(1, cnt + 1);
                stmt.setString(2, danmu.getBv());
                stmt.setLong(3, danmu.getMid());
                stmt.setFloat(4, danmu.getTime());
                stmt.setString(5, danmu.getContent());
                stmt.setTimestamp(6, danmu.getPostTime());
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
        } catch (Exception e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e2) {
                e2.printStackTrace();
            }
        }


        String sql12 = "insert into like_danmu values(?,?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql12)) {
            long cnt = 0;
            for (int i = 0; i < danmuRecords.size(); i++) {
                DanmuRecord danmu = danmuRecords.get(i);
                long[] follow = danmu.getLikedBy();
                for (int j = 0; j < follow.length; j++) {
                    stmt.setLong(1, follow[j]);
                    stmt.setInt(2, i + 1);
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
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private class AThread extends Thread {
        String table_name;
        List<String> bvs;
        List<long[]> mids;

        public AThread(String table_name, List<String> bvs, List<long[]> mids) {
            this.table_name = table_name;
            this.bvs = bvs;
            this.mids = mids;
        }

        @Override
        public void run() {
            Connection conn = null;
            PreparedStatement stmt = null;
            try {
                conn = dataSource.getConnection();
                conn.setAutoCommit(false);
                String sql = "insert into " + table_name + " values (?, ?)";
                stmt = conn.prepareStatement(sql);
                for (int k = 0; k < bvs.size(); k++) {
                    String bv = bvs.get(k);
                    long[] mid = mids.get(k);
                    stmt.setString(2, bv);
                    for (int i = 0; i < mid.length; i += BATCH_SIZE) {
                        for (int j = 0; j < Math.min(BATCH_SIZE, mid.length - i); j++) {
                            stmt.setLong(1, mid[i + j]);
                            stmt.addBatch();
                        }
                        stmt.executeBatch();
                        stmt.clearBatch();
                    }
                }
                conn.commit();
                stmt.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    if (stmt != null)
                        stmt.close();
                    if (conn != null)
                        conn.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    private class BThread extends Thread {
        List<VideoRecord> videos;

        public BThread(List<VideoRecord> videos) {
            this.videos = videos;

        }

        @Override
        public void run() {
            Connection conn = null;
            PreparedStatement stmt = null;
            try {
                conn = dataSource.getConnection();
                conn.setAutoCommit(false);
                String sql = "insert into view_video values (? , ? ,?)";
                stmt = conn.prepareStatement(sql);
                long count = 0;
                for (int i = 0; i < videos.size(); i++) {
                    stmt.setString(2, videos.get(i).getBv());
                    long[] mids = videos.get(i).getViewerMids();
                    float[] times = videos.get(i).getViewTime();
                    for (int j = 0; j < mids.length; j++) {
                        stmt.setLong(1, mids[j]);
                        stmt.setFloat(3, times[j]);
                        stmt.addBatch();
                        count++;
                        if (count % BATCH_SIZE == 0) {
                            stmt.executeBatch();
                            stmt.clearBatch();
                        }
                    }
                    if (count % BATCH_SIZE == 0) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                    }
                }
                if (count % BATCH_SIZE != 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
                conn.commit();
                stmt.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    if (stmt != null)
                        stmt.close();
                    if (conn != null)
                        conn.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    private class CThread extends Thread {
        List<UserRecord> users;

        public CThread(List<UserRecord> users) {
            this.users = users;
        }

        @Override
        public void run() {
            Connection conn = null;
            PreparedStatement stmt = null;
            try {
                conn = dataSource.getConnection();
                conn.setAutoCommit(false);
                String sql = "insert into follow values (?, ?)";
                stmt = conn.prepareStatement(sql);
                long count = 0;
                for (int k = 0; k < users.size(); k++) {
                    stmt.setLong(2, users.get(k).getMid());
                    long[] following = users.get(k).getFollowing();
                    for (int i = 0; i < following.length; i++) {
                        stmt.setLong(1, following[i]);
                        stmt.addBatch();
                        count++;
                        if (count % BATCH_SIZE == 0) {
                            stmt.executeBatch();
                            stmt.clearBatch();
                        }
                    }
                    if (count % BATCH_SIZE == 0) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                    }
                }
                if (count % BATCH_SIZE != 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
                conn.commit();
                stmt.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    if (stmt != null)
                        stmt.close();
                    if (conn != null)
                        conn.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    private class DThread extends Thread {
        List<DanmuRecord> danmuRecords;
        int FIRST_ID;

        public DThread(List<DanmuRecord> danmuRecords, int id) {
            this.danmuRecords = danmuRecords;
            FIRST_ID = id;
        }

        @Override
        public void run() {
            Connection conn = null;
            PreparedStatement stmt = null;
            try {
                conn = dataSource.getConnection();
                conn.setAutoCommit(false);
                String sql = "insert into like_danmu values (?, ?)";
                stmt = conn.prepareStatement(sql);
                long count = 0;
                for (int i = 0; i < danmuRecords.size(); i++) {
                    stmt.setInt(2, FIRST_ID + i);
                    long[] mid = danmuRecords.get(i).getLikedBy();
                    for (int j = 0; j < mid.length; j++) {
                        stmt.setLong(1, mid[j]);
                        stmt.addBatch();
                        count++;
                        if (count % BATCH_SIZE == 0) {
                            stmt.executeBatch();
                            stmt.clearBatch();
                        }
                    }
                    if (count % BATCH_SIZE == 0) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                    }
                }
                if (count % BATCH_SIZE != 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
                conn.commit();
                stmt.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    if (stmt != null)
                        stmt.close();
                    if (conn != null)
                        conn.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    private void insert(String table, List<VideoRecord> dataList) {
        int nThread = Math.min((int) Math.sqrt(dataList.size()), 8);
        ExecutorService executorService = Executors.newFixedThreadPool(nThread);
        try {
            int dealData = 0;
            int ONE_THREAD_DEAL = dataList.size() / nThread + 1;
            int threads = 0;
            List<String> bvs = new ArrayList<>();
            List<long[]> mids = new ArrayList<>();
            for (int k = 0; k < dataList.size(); k++) {
                String bv = dataList.get(k).getBv();
                long[] mid;
                if (table.equals("like_video"))
                    mid = dataList.get(k).getLike();
                else if (table.equals("coin_video"))
                    mid = dataList.get(k).getCoin();
                else
                    mid = dataList.get(k).getFavorite();
                bvs.add(bv);
                mids.add(mid);
                dealData++;
                if (dealData % ONE_THREAD_DEAL == 0) {
                    AThread a = new AThread(table, bvs, mids);
                    executorService.execute(a);
                    bvs = new ArrayList<>();
                    mids = new ArrayList<>();
                }
            }
            AThread a = new AThread(table, bvs, mids);
            executorService.execute(a);
            executorService.shutdown();
            try {
                executorService.awaitTermination(10, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
            executorService.shutdown();
        }
    }


    private void insert(List<VideoRecord> videos) {
        int nThread = Math.min((int) Math.sqrt(videos.size()), 8);
        ExecutorService executorService = Executors.newFixedThreadPool(nThread);
        try {
            int dealData = 0;
            int ONE_THREAD_DEAL = videos.size() / nThread + 1;
            int threads = 0;
            List<VideoRecord> videoRecords = new ArrayList<>();
            for (int k = 0; k < videos.size(); k++) {
                videoRecords.add(videos.get(k));
                dealData++;
                if (dealData % ONE_THREAD_DEAL == 0) {
                    BThread b = new BThread(videoRecords);
                    executorService.execute(b);
                    videoRecords = new ArrayList<>();
                }
            }
            BThread b = new BThread(videoRecords);
            executorService.execute(b);
            executorService.shutdown();
            try {
                executorService.awaitTermination(10, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
            executorService.shutdown();
        }
    }

    private void insertInFollow(List<UserRecord> userRecords) {
        int nThread = Math.min((int) Math.sqrt(userRecords.size()), 8);
        ExecutorService executorService = Executors.newFixedThreadPool(nThread);
        try {
            int dealData = 0;
            int ONE_THREAD_DEAL = userRecords.size() / nThread + 1;
            int threads = 0;
            List<UserRecord> users = new ArrayList<>();
            for (int k = 0; k < userRecords.size(); k++) {
                users.add(userRecords.get(k));
                dealData++;
                if (dealData % ONE_THREAD_DEAL == 0) {
                    CThread c = new CThread(users);
                    executorService.execute(c);
                    users = new ArrayList<>();
                }
            }
            CThread c = new CThread(users);
            executorService.execute(c);
            executorService.shutdown();
            try {
                executorService.awaitTermination(10, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
            executorService.shutdown();
        }
    }

    private void insertInLikeDanmu(List<DanmuRecord> danmuRecords) {
        int nThread = Math.min((int) Math.sqrt(danmuRecords.size()), 8);
        ExecutorService executorService = Executors.newFixedThreadPool(nThread);
        try {
            int dealData = 0;
            int ONE_THREAD_DEAL = danmuRecords.size() / nThread + 1;
            int threads = 0;
            List<DanmuRecord> danmus = new ArrayList<>();
            for (int k = 0; k < danmuRecords.size(); k++) {
                danmus.add(danmuRecords.get(k));
                dealData++;
                if (dealData % ONE_THREAD_DEAL == 0) {
                    DThread d = new DThread(danmus, k - ONE_THREAD_DEAL + 2);
                    executorService.execute(d);
                    danmus = new ArrayList<>();
                }
            }
            DThread d = new DThread(danmus, danmuRecords.size() - danmuRecords.size() % ONE_THREAD_DEAL + 1);
            executorService.execute(d);
            executorService.shutdown();
            try {
                executorService.awaitTermination(10, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
            executorService.shutdown();
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
//
//        , "view_video", "like_video", "fav_video", "coin_video",
//            "danmu_info", "video_info", "follow", "max_mid", "user_auth", "user_info"
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.
        String[] tableName = {"like_danmu" , "view_video", "like_video", "fav_video", "coin_video",
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
