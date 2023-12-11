# CS307 Project Part II

## Basic Information

| Member | Student ID | Contribution Rate |
| ------ | ---------- | ----------------- |
| 袁龙   | 12211308   | 33.33%            |
| 于斯瑶 | 12211655   | 33.33%            |
| 赵钊   | 12110120   | 33.33%            |

**Contribution of work** 
袁龙: E-R diagram, UserService and DanmuSercive implementation and optimization  
于斯瑶: RecommenderService and DatabaseSercive implementation and optimization 
赵钊: Database Design, Table Creation, VideoService implementation and optimization



## Database Design

### 1. E-R Diagram





### 2.  Database Diagram by DataGrip

![](pic/database_diagram.png)

### 3. Table Design Description

We design **11** tables for this project, **10** of them are basic tables and others are help tables to increase efficiency. A brief description of each table is as follows.

**The 10 tables as below are basic tables.**

**user_info**: contains 8 columns of basic information of a user

**user_auth**: contains 4 columns of authentic information of a user

**follow**:  `up_mid` and `fans_mid` is the mid of follower and followee

**video_info**: contains 10 basic columns (including `bv`, `title`, `duration`, `description`, `owner_mid`, `reviewer_mid`, `commit_time`, `review_time`, `public_time`) of information of a video

**like_video**: 2 columns `mid` and `bv` is the mid and bv of the user and the video be liked, also has a constraint `primary key (bv, mid)`

**coin_video**, **fav_video** and **view_video** is the same as **like_video**, but **view_video** contains an extra column `time`, which records where did the user watch in the video

**danmu_info**:  has a self increasing primary key `danmu_id`, and other 5 columns (including `bv`, `mid`, `time`, `content` and `post_time`) of basic information of a danmu

**like_danmu**: records a user (with mid `mid`) liked a danmu (with id `danmu_id`)

**The 1 table as below are help tables mentioned above.**

**max_id**: records the max value of `mid` exists now, in order to increase the efficiency of register a new user (we only need to use (`max_mid` + 1) to be the new user's mid)

### 4. Database Privilege

The user has been used in this project is called `manager`, and the database we used in this project is called `project2`. The user `manager` has privilege to access to all tables and do select, insert, update, delete and truncate operations of schema `public` in database `project2`. Here are the sql statement to create user `manager`:

```sql
create user manager with password '123456';
grant connect on database project2 to manager;
grant usage on schema public to manager;
grant select, insert, update, delete, truncate on all tables in schema public to manager;
commit;
```



## Basic API Specification









## Implement Optimize





